package amqp.outbound;

import java.util.Random;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;

@ApplicationScoped
public class AmqpReplierWithMetadata {

    Random rand = new Random();

    @Incoming("request")
    @Outgoing("reply")
    Message<String> handleRequest(Message<String> request) {
        IncomingAmqpMetadata incoming = request.getMetadata(IncomingAmqpMetadata.class)
                .orElseThrow();
        OutgoingAmqpMetadata outMeta = OutgoingAmqpMetadata.builder()
                .withAddress(incoming.getReplyTo())
                .withCorrelationId(incoming.getId())
                .build();
        String reply = request.getPayload() + "-" + rand.nextInt(100);
        return request.withPayload(reply).addMetadata(outMeta);
    }
}
