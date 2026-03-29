package rabbitmq.outbound;

import java.util.Random;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

@ApplicationScoped
public class RabbitMQReplier {

    Random rand = new Random();

    @Incoming("request")
    @Outgoing("reply")
    Message<String> handleRequest(Message<String> request) {
        IncomingRabbitMQMetadata metadata = request.getMetadata(
                IncomingRabbitMQMetadata.class).get();
        OutgoingRabbitMQMetadata outgoing = OutgoingRabbitMQMetadata.builder()
                .withCorrelationId(metadata.getCorrelationId().get())
                .withRoutingKey(metadata.getReplyTo().get()).build();
        return Message.of("" + rand.nextInt(100), Metadata.of(outgoing));
    }
}
