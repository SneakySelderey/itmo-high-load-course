package ru.quipy.payments.logic

import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import okhttp3.Call
import okhttp3.EventListener
import okhttp3.Response
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

internal class PaymentAttemptLatency(private val account: String) : EventListener.Factory {
    private enum class Outcome(val tag: String) {
        RESPONSE("response"),
        HTTP_ERROR("http_error"),
        PROVIDER_TIMEOUT("provider_timeout"),
        CLIENT_TIMEOUT("client_timeout"),
        IO_ERROR("io_error"),
    }

    private val timers = ConcurrentHashMap<Outcome, Timer>()

    override fun create(call: Call): EventListener = object : EventListener() {
        private lateinit var sample: Timer.Sample
        private var outcome = Outcome.RESPONSE

        override fun callStart(call: Call) {
            sample = Timer.start(Metrics.globalRegistry)
        }

        override fun responseHeadersEnd(call: Call, response: Response) {
            outcome = when {
                response.code == 408 -> Outcome.PROVIDER_TIMEOUT
                response.isSuccessful -> Outcome.RESPONSE
                else -> Outcome.HTTP_ERROR
            }
        }

        override fun callEnd(call: Call) {
            record(outcome)
        }

        override fun callFailed(call: Call, ioe: IOException) {
            record(if (ioe is InterruptedIOException) Outcome.CLIENT_TIMEOUT else Outcome.IO_ERROR)
        }

        private fun record(outcome: Outcome) {
            val timer = timers.computeIfAbsent(outcome) {
                Timer.builder("payment_attempt_duration")
                    .description("HTTP duration of one payment attempt")
                    .tags("account", account, "outcome", outcome.tag)
                    .publishPercentiles(0.5, 0.85, 0.90, 0.95, 0.99)
                    .distributionStatisticExpiry(Duration.ofMinutes(5))
                    .register(Metrics.globalRegistry)
            }
            sample.stop(timer)
        }
    }
}
