package io.smallrye.reactive.messaging.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.microprofile.reactive.messaging.*;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

@ApplicationScoped
public class ProducingMessageWithHeaderBean {

    private final AtomicInteger counter = new AtomicInteger();

    @Incoming("data")
    @Outgoing("output-2")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(Message<Integer> input) {
        List<RecordHeader> list = Arrays.asList(
                new RecordHeader("hello", "clement".getBytes()),
                new RecordHeader("count", Integer.toString(counter.incrementAndGet()).getBytes()));
        return Message.of(
                input.getPayload() + 1,
                Metadata.of(
                        OutgoingKafkaRecordMetadata.builder().withKey(Integer.toString(input.getPayload()))
                                .withHeaders(list).build()),
                input::ack);
    }

    @Outgoing("data")
    public Publisher<Integer> source() {
        return Multi.createFrom().range(0, 10);
    }

}
