package io.smallrye.reactive.messaging.kafka;

import io.reactivex.Flowable;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class ProducingKafkaMessageBean {

    private AtomicInteger counter = new AtomicInteger();

    @Incoming("data")
    @Outgoing("output-2")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(Message<Integer> input) {
        OutgoingKafkaRecordMetadata metadata = OutgoingKafkaRecordMetadata.builder()
            .withKey(Integer.toString(input.getPayload()))
            .withHeaders(Arrays.asList(
                new RecordHeader("hello", "clement".getBytes()),
                new RecordHeader("count", Integer.toString(counter.incrementAndGet()).getBytes()))).build();
        return Message.<Integer>newBuilder()
            .payload(input.getPayload() + 1)
            .metadata(metadata)
            .ack(input::ack).build();
    }

    @Outgoing("data")
    public Publisher<Integer> source() {
        return Flowable.range(0, 10);
    }

}
