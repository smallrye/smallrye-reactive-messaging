package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.providers.helpers.GenericPayloadConverter;

public class GenericPayloadTest extends WeldTestBaseWithoutTails {

    @Test
    public void testIncomingGenericPayload() {
        addBeanClass(MyCollector.class);
        addBeanClass(GenericPayloadConverter.class);
        addBeanClass(GenericPayloadConsumer.class);

        initialize();

        GenericPayloadConsumer consumer = get(GenericPayloadConsumer.class);
        await().untilAsserted(() -> assertThat(consumer.received())
                .hasSize(10)
                .extracting(GenericPayload::getPayload)
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @ApplicationScoped
    public static class GenericPayloadConsumer {

        List<GenericPayload<Integer>> payloads = new CopyOnWriteArrayList<>();

        @Incoming("count")
        void consume(GenericPayload<Integer> payload) {
            payloads.add(payload);
        }

        public List<GenericPayload<Integer>> received() {
            return payloads;
        }
    }

    @Test
    public void testOutgoingGenericPayload() {
        addBeanClass(MyCollector.class);
        addBeanClass(GenericPayloadProducer.class);

        initialize();

        GenericPayloadProducer producer = get(GenericPayloadProducer.class);
        MyCollector collector = get(MyCollector.class);
        await().untilAsserted(() -> assertThat(collector.messages())
                .hasSize(10)
                .allSatisfy(m -> assertThat(m.getMetadata(Object.class)).hasValue(producer.metadata()))
                .extracting(Message::getPayload)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    }

    @ApplicationScoped
    public static class GenericPayloadProducer {

        Object metadata = new Object();

        @Outgoing("sink")
        Multi<GenericPayload<String>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> GenericPayload.of("" + i, Metadata.of(metadata)));
        }

        public Object metadata() {
            return metadata;
        }

    }

    @Test
    public void testProcessorGenericPayload() {
        addBeanClass(MyCollector.class);
        addBeanClass(GenericPayloadProcessor.class);
        addBeanClass(GenericPayloadConverter.class);

        initialize();

        GenericPayloadProcessor processor = get(GenericPayloadProcessor.class);
        MyCollector collector = get(MyCollector.class);
        await().untilAsserted(() -> assertThat(collector.messages())
                .hasSize(10)
                .allSatisfy(m -> assertThat(m.getMetadata(Object.class).get()).isEqualTo(processor.metadata()))
                .extracting(Message::getPayload)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    }

    @ApplicationScoped
    private static class GenericPayloadProcessor {
        Object metadata = new Object();

        @Incoming("count")
        @Outgoing("sink")
        GenericPayload<String> process(GenericPayload<Integer> payload) {
            return payload.withPayload("" + payload.getPayload())
                    .withMetadata(Metadata.of(metadata));
        }

        public Object metadata() {
            return metadata;
        }

    }

    @Test
    public void testMultiProcessorGenericPayload() {
        addBeanClass(MyCollector.class);
        addBeanClass(GenericPayloadMultiProcessor.class);
        addBeanClass(GenericPayloadConverter.class);

        initialize();

        GenericPayloadMultiProcessor processor = get(GenericPayloadMultiProcessor.class);
        MyCollector collector = get(MyCollector.class);
        await().untilAsserted(() -> assertThat(collector.messages())
                .hasSize(10)
                .allSatisfy(m -> assertThat(m.getMetadata(Object.class).get()).isEqualTo(processor.metadata()))
                .extracting(Message::getPayload)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    }

    @ApplicationScoped
    private static class GenericPayloadMultiProcessor {
        Object metadata = new Object();

        @Incoming("count")
        @Outgoing("sink")
        Multi<GenericPayload<String>> process(GenericPayload<Integer> payload) {
            return Multi.createFrom().items(payload.withPayload("" + payload.getPayload())
                    .withMetadata(Metadata.of(metadata)));
        }

        public Object metadata() {
            return metadata;
        }

    }

    @Test
    public void testStreamTransformerGenericPayload() {
        addBeanClass(MyCollector.class);
        addBeanClass(GenericPayloadStreamTransformer.class);
        addBeanClass(GenericPayloadConverter.class);

        initialize();

        GenericPayloadStreamTransformer processor = get(GenericPayloadStreamTransformer.class);
        MyCollector collector = get(MyCollector.class);
        await().untilAsserted(() -> assertThat(collector.messages())
                .hasSize(10)
                .allSatisfy(m -> assertThat(m.getMetadata(Object.class).get()).isEqualTo(processor.metadata()))
                .extracting(Message::getPayload)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    }

    @ApplicationScoped
    private static class GenericPayloadStreamTransformer {
        Object metadata = new Object();

        @Incoming("count")
        @Outgoing("sink")
        Multi<GenericPayload<String>> process(Multi<Integer> payload) {
            return payload
                    .map(i -> GenericPayload.of("" + i, Metadata.of(metadata)));
        }

        public Object metadata() {
            return metadata;
        }

    }

    @Test
    public void testEmitterGenericPayloadNotSupported() {
        addBeanClass(MyCollector.class);
        addBeanClass(GenericPayloadEmitter.class);
        addBeanClass(GenericPayloadConverter.class);

        initialize();

        GenericPayloadEmitter emitter = get(GenericPayloadEmitter.class);
        MyCollector collector = get(MyCollector.class);

        emitter.send10();

        await().untilAsserted(() -> assertThat(collector.messages())
                .hasSize(10)
                .allSatisfy(m -> assertThat(m.getMetadata(Object.class)).isEmpty())
                .extracting(m -> (GenericPayload) (Object) m.getPayload())
                .allSatisfy(p -> assertThat(p).isInstanceOf(GenericPayload.class)));
    }

    @ApplicationScoped
    private static class GenericPayloadEmitter {

        Object metadata = new Object();

        @Inject
        @Channel("sink")
        Emitter<GenericPayload<String>> emitter;

        void send10() {
            for (int i = 0; i < 10; i++) {
                emitter.send(GenericPayload.of("" + i, Metadata.of(metadata)));
            }
        }

        public Object metadata() {
            return metadata;
        }
    }
}
