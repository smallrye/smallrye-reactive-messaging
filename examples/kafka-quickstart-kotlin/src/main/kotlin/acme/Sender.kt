package acme

import java.util.concurrent.*

import javax.enterprise.context.ApplicationScoped

import org.eclipse.microprofile.reactive.messaging.Outgoing

import io.smallrye.reactive.messaging.kafka.KafkaMessage

@ApplicationScoped
open class Sender {

    private val executor = Executors.newSingleThreadScheduledExecutor()

    @Outgoing("data")
    open fun send(): CompletionStage<KafkaMessage<String, String>> {
        val future = CompletableFuture<KafkaMessage<String, String>>()
        delay(Runnable { future.complete(KafkaMessage.of("kafka", "key", "hello from MicroProfile")) })
        return future
    }

    private fun delay(runnable: Runnable) {
        executor.schedule(runnable, 5, TimeUnit.SECONDS)
    }

}
