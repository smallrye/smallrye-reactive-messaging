package acme

import java.util.concurrent.*

import jakarta.enterprise.context.ApplicationScoped

import org.eclipse.microprofile.reactive.messaging.Outgoing

import io.smallrye.reactive.messaging.kafka.KafkaMessage
import io.smallrye.reactive.messaging.kafka.KafkaRecord

@ApplicationScoped
open class Sender {

    private val executor = Executors.newSingleThreadScheduledExecutor()

    @Outgoing("data")
    open fun send(): CompletionStage<KafkaRecord<String, String>> {
        val future = CompletableFuture<KafkaRecord<String, String>>()
        delay(Runnable { future.complete(KafkaRecord.of("kafka", "key", "hello from MicroProfile")) })
        return future
    }

    private fun delay(runnable: Runnable) {
        executor.schedule(runnable, 5, TimeUnit.SECONDS)
    }

}
