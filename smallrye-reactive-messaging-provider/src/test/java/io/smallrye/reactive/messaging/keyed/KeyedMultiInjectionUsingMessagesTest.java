package io.smallrye.reactive.messaging.keyed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class KeyedMultiInjectionUsingMessagesTest extends WeldTestBaseWithoutTails {

    static class SuccessCaseParameters implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            // Tuple of: Name, List of bean classes, Success / Failure
            return Stream.of(
                    Arguments.of("Extractor using payload",
                            List.of(AppWithDefault.class, ExtractorFromPayload.class, PayloadSource.class)),
                    Arguments.of("Extractor using metadata",
                            List.of(AppWithDefault.class, ExtractorFromMetadata.class, MessageSource.class)),
                    Arguments.of("Two matching extractors",
                            List.of(AppWithDefault.class, ExtractorFromMetadata.class,
                                    ExtractorFromMetadataWithHigherPriority.class, MessageSource.class)),
                    Arguments.of("Two matching extractors (reversed order)",
                            List.of(AppWithDefault.class, ExtractorFromMetadataWithHigherPriority.class,
                                    ExtractorFromMetadata.class, MessageSource.class)),
                    Arguments.of("Application using @Keyed",
                            List.of(AppWithKeyed.class, ExtractorFromPayloadSelectedUsingKeyed.class,
                                    PayloadSource.class)));
        }
    }

    static class FailingCaseParameters implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            // Tuple of: Name, List of bean classes, Success / Failure
            return Stream.of(
                    Arguments.of("No extractor",
                            List.of(AppWithDefault.class, MessageSource.class)),
                    Arguments.of("No matching extractor",
                            List.of(AppWithDefault.class, ExtractorFromMetadata.class, PayloadSource.class)),
                    Arguments.of("Application using @Keyed but not extractor",
                            List.of(AppWithKeyed.class, PayloadSource.class)),
                    Arguments.of("Application using @Keyed but no matching",
                            List.of(AppWithKeyed.class, ExtractorFromMetadata.class, PayloadSource.class)),
                    Arguments.of("Extractor failing in `canExtract`",
                            List.of(AppWithDefault.class, ExtractorFromPayloadThrowingInCanExtract.class, PayloadSource.class)),
                    Arguments.of("Extractor failing in `extractKey`",
                            List.of(AppWithDefault.class, ExtractorFromPayloadThrowingWhileExtractingTheKey.class,
                                    PayloadSource.class)),
                    Arguments.of("Extractor failing in `extractValue`",
                            List.of(AppWithDefault.class, ExtractorFromPayloadThrowingWhileExtractingTheValue.class,
                                    PayloadSource.class)),
                    Arguments.of("Extractor selected with @Keyed failing in `extractKey`",
                            List.of(AppWithKeyed.class,
                                    ExtractorFromPayloadSelectedUsingKeyedFailingWhileExtractingTheKey.class,
                                    PayloadSource.class)),
                    Arguments.of("Extractor selected with @Keyed  failing in `extractValue`",
                            List.of(AppWithKeyed.class,
                                    ExtractorFromPayloadSelectedUsingKeyedFailingWhileExtractingTheValue.class,
                                    PayloadSource.class)),
                    Arguments.of("Extractor using payload returning wrong key type",
                            List.of(AppWithDefault.class, ExtractorFromPayloadReturningWrongKeyType.class,
                                    PayloadSource.class)),
                    Arguments.of("Extractor using payload returning wrong value type",
                            List.of(AppWithDefault.class, ExtractorFromPayloadReturningWrongValueType.class,
                                    PayloadSource.class))

            );
        }
    }

    @ArgumentsSource(SuccessCaseParameters.class)
    @ParameterizedTest(name = "{0}")
    void testSuccessCase(String name, List<Class<?>> classes) {
        addBeanClass(Sink.class);
        classes.forEach(this::addBeanClass);

        // Failures are captured during the initialization because these tests are not async (the sources are immediate)

        try {
            initialize();
        } catch (Exception e) {
            Assertions.fail("Initialization not expected to fail", e);
            return;
        }

        Sink sink = get(Sink.class);
        Counted source = get(Counted.class);
        await().until(() -> sink.list().size() == 11);
        await().until(() -> {
            System.out.println("count: " + source.count());
            return source.count() == 8;
        }); // No 0.
        assertThat(sink.list())
                .containsExactlyInAnyOrder(
                        "A-0", "B-0", "C-0",
                        "A-1", "A-2", "A-3", "A-4",
                        "B-1", "B-2", "C-1", "C-2");
    }

    @ArgumentsSource(FailingCaseParameters.class)
    @ParameterizedTest(name = "{0}")
    void testFailingCases(String name, List<Class<?>> classes) {
        addBeanClass(Sink.class);
        classes.forEach(this::addBeanClass);

        // Failures are captured during the initialization because these tests are not async (the sources are immediate)
        try {
            initialize();
        } catch (Exception e) {
            return;
        }

        Assertions.fail("Initialization expected to fail");
    }

    @ApplicationScoped
    public static class PayloadSource implements Counted {
        private final AtomicInteger count = new AtomicInteger();

        @Override
        public int count() {
            return count.get();
        }

        @Outgoing("in")
        Multi<Message<String>> source() {
            return Multi.createFrom().items(
                    "a-1", "b-1", "b-2", "a-2", "a-3", "c-1", "c-2", "a-4")
                    .onItem().transform(s -> Message.of(s).withAck(() -> {
                        count.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    }));
        }
    }

    public static class Key {
        public final String key;

        public Key(String key) {
            this.key = key;
        }

    }

    public interface Counted {
        int count();
    }

    @ApplicationScoped
    public static class MessageSource implements Counted {

        private AtomicInteger counter = new AtomicInteger();

        @Outgoing("in")
        Multi<Message<String>> source() {
            Supplier<CompletionStage<Void>> supplier = () -> {
                counter.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            };
            return Multi.createFrom().items(
                    Message.of("1").addMetadata(new Key("a")).withAck(supplier),
                    Message.of("1").addMetadata(new Key("b")).withAck(supplier),
                    Message.of("2").addMetadata(new Key("b")).withAck(supplier),
                    Message.of("2").addMetadata(new Key("a")).withAck(supplier),
                    Message.of("3").addMetadata(new Key("a")).withAck(supplier),
                    Message.of("1").addMetadata(new Key("c")).withAck(supplier),
                    Message.of("2").addMetadata(new Key("c")).withAck(supplier),
                    Message.of("4").addMetadata(new Key("a")).withAck(supplier));
        }

        @Override
        public int count() {
            return counter.get();
        }
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

    private static class Container {
        public final Message<String> message;
        public final int count;

        private Container(Message<String> message, int count) {
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
                    .select().distinct(Comparator.comparing(Message::getPayload))
                    .onItem().scan(() -> new Container(Message.of(null), 0),
                            (cont, s) -> new Container(s, cont.count + 1))
                    .map(cont -> cont.message.withPayload(keyed.key().toUpperCase() + "-" + cont.count));
        }

    }

    @ApplicationScoped
    public static class AppWithKeyed {

        @Incoming("in")
        @Outgoing("out")
        public Multi<Message<String>> reshape(
                @Keyed(ExtractorFromPayloadSelectedUsingKeyed.class) KeyedMulti<String, Message<String>> keyed) {
            assertThat(keyed.key()).isNotNull().isNotBlank();
            return keyed
                    .select().distinct(Comparator.comparing(Message::getPayload))
                    .onItem().scan(() -> new Container(Message.of(null), 0),
                            (cont, s) -> new Container(s, cont.count + 1))
                    .map(cont -> cont.message.withPayload(keyed.key().toUpperCase() + "-" + cont.count));
        }

    }

    @ApplicationScoped
    public static class ExtractorFromPayload implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            return keyType.equals(String.class) && valueType.equals(String.class);
        }

        @Override
        public String extractKey(Message<?> message, Type keyType) {
            String string = message.getPayload().toString();
            return string.substring(0, string.indexOf("-"));
        }

        @Override
        public String extractValue(Message<?> message, Type valueType) {
            String string = message.getPayload().toString();
            return string.substring(string.indexOf("-") + 1);
        }
    }

    @ApplicationScoped
    public static class ExtractorFromPayloadThrowingInCanExtract extends ExtractorFromPayload implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            throw new IllegalStateException("expected");
        }

    }

    @ApplicationScoped
    public static class ExtractorFromPayloadThrowingWhileExtractingTheKey extends ExtractorFromPayload
            implements KeyValueExtractor {

        @Override
        public String extractKey(Message<?> message, Type keyType) {
            throw new IllegalStateException("expected");
        }

    }

    @ApplicationScoped
    public static class ExtractorFromPayloadThrowingWhileExtractingTheValue extends ExtractorFromPayload
            implements KeyValueExtractor {

        @Override
        public String extractValue(Message<?> message, Type valueType) {
            throw new IllegalStateException("expected");
        }

    }

    @ApplicationScoped
    public static class ExtractorFromPayloadSelectedUsingKeyed extends ExtractorFromPayload implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            throw new UnsupportedOperationException("Should not be called");
        }

    }

    @ApplicationScoped
    public static class ExtractorFromPayloadSelectedUsingKeyedFailingWhileExtractingTheKey extends ExtractorFromPayload
            implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public String extractKey(Message<?> message, Type keyType) {
            throw new IllegalStateException("expected");
        }

    }

    @ApplicationScoped
    public static class ExtractorFromPayloadSelectedUsingKeyedFailingWhileExtractingTheValue extends ExtractorFromPayload
            implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public String extractValue(Message<?> message, Type valueType) {
            throw new IllegalStateException("expected");
        }

    }

    @ApplicationScoped
    public static class ExtractorFromMetadata implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            return msg.getMetadata(Key.class).isPresent()
                    && keyType.equals(String.class) && valueType.equals(String.class);
        }

        @Override
        public String extractKey(Message<?> message, Type keyType) {
            return message.getMetadata(Key.class).map(k -> k.key).orElseThrow();
        }

        @Override
        public String extractValue(Message<?> message, Type valueType) {
            return (String) message.getPayload();
        }

        @Override
        public int getPriority() {
            return 50;
        }
    }

    @ApplicationScoped
    public static class ExtractorFromMetadataWithHigherPriority implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            return msg.getMetadata(Key.class).isPresent()
                    && keyType.equals(String.class) && valueType.equals(String.class);
        }

        @Override
        public String extractKey(Message<?> message, Type keyType) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public String extractValue(Message<?> message, Type valueType) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public int getPriority() {
            return 200;
        }
    }

    @ApplicationScoped
    public static class ExtractorFromPayloadReturningWrongKeyType implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            return keyType.equals(String.class) && valueType.equals(String.class);
        }

        @Override
        public List<String> extractKey(Message<?> message, Type keyType) {
            String string = message.getPayload().toString();
            return List.of(string.substring(0, string.indexOf("-")));
        }

        @Override
        public String extractValue(Message<?> message, Type valueType) {
            String string = message.getPayload().toString();
            return string.substring(string.indexOf("-") + 1);
        }
    }

    @ApplicationScoped
    public static class ExtractorFromPayloadReturningWrongValueType implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
            return keyType.equals(String.class) && valueType.equals(String.class);
        }

        @Override
        public String extractKey(Message<?> message, Type keyType) {
            String string = message.getPayload().toString();
            return string.substring(string.indexOf("-") + 1);
        }

        @Override
        public List<String> extractValue(Message<?> message, Type valueType) {
            String string = message.getPayload().toString();
            return List.of(string.substring(0, string.indexOf("-")));
        }
    }
}
