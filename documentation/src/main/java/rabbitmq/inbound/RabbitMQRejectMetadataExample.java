package rabbitmq.inbound;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.rabbitmq.RabbitMQRejectMetadata;

@ApplicationScoped
public class RabbitMQRejectMetadataExample {

    // <code>
    @Incoming("in")
    public CompletionStage<Void> consume(Message<String> message) {
        return message.nack(new Exception("Failed!"), Metadata.of(
                new RabbitMQRejectMetadata(true)));
    }
    // </code>

}
