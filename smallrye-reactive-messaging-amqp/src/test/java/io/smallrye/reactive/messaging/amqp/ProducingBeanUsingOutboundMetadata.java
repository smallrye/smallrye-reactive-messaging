package io.smallrye.reactive.messaging.amqp;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class ProducingBeanUsingOutboundMetadata {

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(Message<Integer> input) {
        OutgoingAmqpMetadata metadata = OutgoingAmqpMetadata.builder()
                .withSubject("metadata-subject")
                .withAddress("metadata-address")
                .build();

        return Message.of(input.getPayload() + 1, input::ack).addMetadata(metadata);
    }

    @Outgoing("data")
    public Publisher<Integer> source() {
        return Multi.createFrom().range(0, 10);
    }

}
