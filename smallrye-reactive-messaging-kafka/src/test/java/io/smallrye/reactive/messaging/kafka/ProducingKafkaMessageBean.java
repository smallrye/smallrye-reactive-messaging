package io.smallrye.reactive.messaging.kafka;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class ProducingKafkaMessageBean {

    private final AtomicInteger counter = new AtomicInteger();

    @Incoming("data")
    @Outgoing("output-2")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public KafkaRecord<String, Integer> process(Message<Integer> input) {
        return KafkaRecord.of(
                Integer.toString(input.getPayload()), input.getPayload() + 1)
                .withAck(input::ack)
                .withHeader("hello", "clement")
                .withHeader("count", Integer.toString(counter.incrementAndGet()));
    }

    @Outgoing("data")
    public Publisher<Integer> source() {
        return Multi.createFrom().range(0, 10);
    }

}
