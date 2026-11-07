package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
        const val MAX_ATTEMPTS = 3
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            val worstProcessingTimeMillis = 2 * account.properties().averageProcessingTime.toMillis()
            var attempt = 1
            while (!account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)) {
                if (attempt >= MAX_ATTEMPTS || now() + worstProcessingTimeMillis > deadline) break
                attempt++
            }
        }
    }
}