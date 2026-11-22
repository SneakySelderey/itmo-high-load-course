package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private const val TIMEOUT_RESPONSE_GRACE_MILLIS = 200L

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .eventListenerFactory(PaymentAttemptLatency(accountName))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelRequestsLimiter = Semaphore(parallelRequests, true)

    private val outgoing = Metrics.counter("payment_requests", "stage", "outgoing")
    private val retries = Metrics.counter("payment_retries", "account", accountName)
    private val timeouts = Metrics.counter("payment_attempt_timeouts", "account", accountName)
    private val attemptTimeoutMillis = (requestAverageProcessingTime.toMillis() * 3 / 2).coerceAtLeast(1)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, attempt: Int): Boolean {
        val acquireTimeout = deadline - now() - requestAverageProcessingTime.toMillis()
        val acquired = acquireTimeout > 0 && parallelRequestsLimiter.tryAcquire(acquireTimeout, TimeUnit.MILLISECONDS)

        try {
            while (true) {
                if (!acquired || now() + requestAverageProcessingTime.toMillis() >= deadline) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), null, reason = "Deadline exceeded")
                    }
                    logger.warn("[$accountName] Deadline exceeded for payment $paymentId")

                    return false
                }

                if (rateLimiter.tick()) break

                Thread.sleep(10L)
            }

            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            try {
                val timeoutMillis = minOf(attemptTimeoutMillis, deadline - now() - TIMEOUT_RESPONSE_GRACE_MILLIS)
                if (timeoutMillis <= 0) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                    }
                    return false
                }
                val providerTimeout = Duration.ofMillis(timeoutMillis)
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$providerTimeout")
                    post(emptyBody)
                }.build()
                val call = client.newCall(request)
                val clientTimeoutMillis = minOf(timeoutMillis + TIMEOUT_RESPONSE_GRACE_MILLIS, deadline - now())
                if (clientTimeoutMillis <= 0) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                    }
                    return false
                }
                call.timeout().timeout(clientTimeoutMillis, TimeUnit.MILLISECONDS)

                outgoing.increment()
                if (attempt > 1) retries.increment()

                call.execute().use { response ->
                    if (response.code == 408) {
                        timeouts.increment()
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Provider timeout.")
                        }
                        return false
                    }
                    val responseBody = response.body?.string()
                    val body = try {
                        mapper.readValue(responseBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}", e)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    return body.result
                }
            } catch (e: Exception) {
                when (e) {
                    is InterruptedIOException -> {
                        timeouts.increment()
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
            return false
        } finally {
            if (acquired) parallelRequestsLimiter.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun properties() = properties

}

public fun now() = System.currentTimeMillis()