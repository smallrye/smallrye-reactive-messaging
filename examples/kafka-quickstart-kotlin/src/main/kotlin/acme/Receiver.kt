package acme

import io.smallrye.reactive.messaging.kafka.KafkaRecord
import org.eclipse.microprofile.reactive.messaging.Incoming
import java.util.concurrent.CompletionStage
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
open class Receiver {

    @Incoming("kafka")
    open fun consume(message: KafkaRecord<String, String>): CompletionStage<Void> {
        val payload = message.payload
        val key = message.key
        val headers = message.headers
        val partition = message.partition
        val timestamp = message.timestamp
        println("received: " + payload + " from topic " + message.topic)
        return message.ack()
    }

}
