package io.smallrye.reactive.messaging.kafka.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.keyed.Keyed;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;

class KeyValueMessageFromRecordExtractorTest extends KafkaCompanionTestBase {

    static class MyParameters implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            // Tuple of: Name, List of bean classes, Success / Failure
            return Stream.of(
                    Arguments.of("Use the default extractor", List.of(AppWithDefault.class)),
                    Arguments.of("Use the custom extractor selected with @Keyed",
                            List.of(AppWithKeyed.class, ExtractorSelectedUsingKeyed.class)),
                    Arguments.of("Use the default extractor selected with @Keyed", List.of(AppWithKeyedUsingDefault.class)),
                    Arguments.of("Use the custom extractor",
                            List.of(AppWithDefaultButDoBeUSedWithCustom.class, CustomExtractor.class)));
        }
    }

    @DisplayName("KeyedMultiOfMessageTest")
    @ArgumentsSource(MyParameters.class)
    @ParameterizedTest(name = "{0}")
    void test(String name, List<Class<?>> classes) {
        addBeans(Sink.class, KeyValueFromKafkaRecordExtractor.class);
        classes.forEach(this::addBeans);

        runApplication(kafkaConfig("mp.messaging.incoming.in", false)
                .with("connector", "smallrye-kafka")
                .with("topic", topic)
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("auto.offset.reset", "earliest"));

        Sink sink = get(Sink.class);

        companion.produceStrings().fromMulti(
                Multi.createFrom().items(
                        new ProducerRecord<>(topic, "a", "1"),
                        new ProducerRecord<>(topic, "b", "1"),
                        new ProducerRecord<>(topic, "b", "2"),
                        new ProducerRecord<>(topic, "a", "2"),
                        new ProducerRecord<>(topic, "a", "3"),
                        new ProducerRecord<>(topic, "c", "1"),
                        new ProducerRecord<>(topic, "c", "2"),
                        new ProducerRecord<>(topic, "a", "4")))
                .awaitCompletion();

        await().until(() -> sink.list().size() == 11);
        assertThat(sink.list())
                .containsExactlyInAnyOrder(
                        "A-0", "B-0", "C-0",
                        "A-1", "A-2", "A-3", "A-4",
                        "B-1", "B-2", "C-1", "C-2");
    }

    @ApplicationScoped
    public static class Sink {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("out")
        void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    public static class Container {
        Message<String> message;
        int count;

        public Container(Message<String> message, int count) {
            this.message = message;
            this.count = count;
        }
    }

    @ApplicationScoped
    public static class AppWithDefault {

        @Incoming("in")
        @Outgoing("out")
        public Multi<Message<String>> reshape(KeyedMulti<String, Message<String>> keyed) {
            assertThat(keyed.key()).isNotNull().isNotBlank();
            return keyed
                    .select().distinct()
                    .onItem().scan(() -> new Container(Message.of(null), 0),
                            (cont, msg) -> new Container(msg, cont.count + 1))
                    .map(cont -> cont.message.withPayload(keyed.key().toUpperCase() + "-" + cont.count));
        }

    }

    @ApplicationScoped
    public static class AppWithDefaultButDoBeUSedWithCustom {

        @Incoming("in")
        @Outgoing("out")
        //        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Multi<Message<String>> reshape(KeyedMulti<String, Message<String>> keyed) {
            assertThat(keyed.key()).isNotNull().isNotBlank();
            return keyed
                    .select().distinct()
                    .onItem().scan(() -> new Container(Message.of(null), 0),
                            (cont, msg) -> new Container(msg, cont.count + 1))
                    .map(cont -> cont.message.withPayload(keyed.key().toUpperCase() + "-" + cont.count)); // The extractor should to the uppercase
        }

    }

    @ApplicationScoped
    public static class AppWithKeyedUsingDefault {

        @Incoming("in")
        @Outgoing("out")
        //        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Multi<Message<String>> reshape(
                @Keyed(KeyValueFromKafkaRecordExtractor.class) KeyedMulti<String, Message<String>> keyed) {
            assertThat(keyed.key()).isNotNull().isNotBlank();
            return keyed
                    .select().distinct()
                    .onItem().scan(() -> new Container(Message.of(null), 0),
                            (cont, msg) -> new Container(msg, cont.count + 1))
                    .map(cont -> cont.message.withPayload(keyed.key().toUpperCase() + "-" + cont.count)); // The extractor should to the uppercase
        }

    }

    @ApplicationScoped
    public static class AppWithKeyed {

        @Incoming("in")
        @Outgoing("out")
        public Multi<String> reshape(@Keyed(ExtractorSelectedUsingKeyed.class) KeyedMulti<String, String> keyed) {
            assertThat(keyed.key()).isNotNull().isNotBlank();
            return keyed
                    .select().distinct()
                    .onItem().scan(AtomicInteger::new, (count, s) -> {
                        count.incrementAndGet();
                        return count;
                    })
                    .map(s -> keyed.key() + "-" + s.get()); // no uppercase, the extractor does it
        }

    }

    @ApplicationScoped
    public static class ExtractorSelectedUsingKeyed implements KeyValueExtractor {
        @Override
        public boolean canExtract(Message<?> first, Type keyType, Type valueType) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public String extractKey(Message<?> message, Type keyType) {
            return ((String) message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .orElseThrow()
                    .getKey()).toUpperCase();
        }

        @Override
        public Object extractValue(Message<?> message, Type valueType) {
            return message.getPayload();
        }
    }

    @ApplicationScoped
    public static class CustomExtractor implements KeyValueExtractor {
        @Override
        public boolean canExtract(Message<?> first, Type keyType, Type valueType) {
            return first.getMetadata(IncomingKafkaRecordMetadata.class).isPresent();
        }

        @Override
        public String extractKey(Message<?> message, Type keyType) {
            return ((String) message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .orElseThrow()
                    .getKey()).toUpperCase();
        }

        @Override
        public Object extractValue(Message<?> message, Type valueType) {
            return message.getPayload();
        }

        @Override
        public int getPriority() {
            return DEFAULT_PRIORITY - 10;
        }
    }
}
