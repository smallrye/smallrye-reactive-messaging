package acme

import io.smallrye.reactive.messaging.kafka.KafkaMessage
import org.eclipse.microprofile.reactive.messaging.Incoming
import java.util.concurrent.CompletionStage
import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
open class Receiver {

    @Incoming("kafka")
    open fun consume(message: KafkaMessage<String, String>): CompletionStage<Void> {
        val payload = message.payload
        val key = message.key
        val headers = message.kafkaHeaders
        val partition = message.partition
        val timestamp = message.timestamp
        println("received: " + payload + " from topic " + message.topic)
        return message.ack()
    }

}
