package acme;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

@ApplicationScoped
public class Replier {

    @Incoming("requests")
    @Outgoing("replies")
    public Message<String> reply(Message<String> request) {
        IncomingRabbitMQMetadata metadata = request.getMetadata(
                IncomingRabbitMQMetadata.class).get();
        System.out.println("received request: " + request.getPayload() + " replyTo: " + metadata.getReplyTo().get());
        OutgoingRabbitMQMetadata outgoing = OutgoingRabbitMQMetadata.builder()
                .withCorrelationId(metadata.getCorrelationId().get())
                .withRoutingKey(metadata.getReplyTo().get()).build();
        return Message.of("Reply to " + request.getPayload(), Metadata.of(outgoing));
    }

}
