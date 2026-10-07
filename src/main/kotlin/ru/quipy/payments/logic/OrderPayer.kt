package ru.quipy.payments.logic

import io.micrometer.core.instrument.Metrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class TooManyRequestsException(val retryAfter: Long) : RuntimeException()

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var accounts: List<PaymentExternalSystemAdapter>

    private val account by lazy { accounts.first().properties() }

    private val queue by lazy { Semaphore(2 * account.parallelRequests) }

    private val retryAfterMillis by lazy { account.averageProcessingTime.toMillis() / account.parallelRequests }

    private val accepted = Metrics.counter("payment_requests", "stage", "accepted")
    private val rejected = Metrics.counter("payment_requests", "stage", "rejected")
    private val finished = Metrics.counter("payment_requests", "stage", "finished")

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (!queue.tryAcquire()) {
            rejected.increment()
            throw TooManyRequestsException(System.currentTimeMillis() + retryAfterMillis)
        }
        accepted.increment()

        val createdAt = System.currentTimeMillis()
        paymentExecutor.submit {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } finally {
                queue.release()
                finished.increment()
            }
        }
        return createdAt
    }
}