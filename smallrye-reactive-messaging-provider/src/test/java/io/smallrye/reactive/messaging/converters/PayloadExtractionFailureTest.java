package io.smallrye.reactive.messaging.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

/**
 * A message whose payload cannot be extracted (e.g. a connector failing to decode it lazily in
 * {@link Message#getPayload()}) must be nacked and skipped, instead of failing the whole stream.
 */
public class PayloadExtractionFailureTest extends WeldTestBaseWithoutTails {

    private static final String POISON = "<poison>";

    @Test
    void payloadMethodWithoutConverter() {
        addBeanClass(Source.class, StringSink.class);
        initialize();

        Source source = get(Source.class);
        await().untilAsserted(() -> {
            assertThat(get(StringSink.class).received()).containsExactly("Luke", "Leia", "Trinity");
            assertThat(source.acks()).isEqualTo(3);
            assertThat(source.nacks()).isEqualTo(2);
        });
        assertThat(source.nackReasons())
                .allSatisfy(t -> assertThat(t).isInstanceOf(IllegalStateException.class).hasMessage("cannot decode"));
    }

    @Test
    void blockingPayloadMethodWithoutConverter() {
        addBeanClass(Source.class, BlockingStringSink.class);
        initialize();

        Source source = get(Source.class);
        await().untilAsserted(() -> {
            assertThat(get(BlockingStringSink.class).received()).containsExactly("Luke", "Leia", "Trinity");
            assertThat(source.acks()).isEqualTo(3);
            assertThat(source.nacks()).isEqualTo(2);
        });
    }

    @Test
    void payloadMethodWithConverter() {
        addBeanClass(Source.class, StringToPersonConverter.class, PersonProcessor.class, PersonSink.class);
        initialize();

        Source source = get(Source.class);
        await().untilAsserted(() -> {
            assertThat(get(PersonSink.class).received()).containsExactly("LUKE", "LEIA", "TRINITY");
            assertThat(source.acks()).isEqualTo(3);
            assertThat(source.nacks()).isEqualTo(2);
        });
    }

    @Test
    void messageMethod() {
        addBeanClass(Source.class, MessageSink.class);
        initialize();

        Source source = get(Source.class);
        await().untilAsserted(() -> {
            assertThat(get(MessageSink.class).received()).containsExactly("Luke", "Leia", "Trinity");
            assertThat(source.acks()).isEqualTo(3);
            assertThat(source.nacks()).isEqualTo(2);
        });
    }

    /**
     * A failed nack (e.g. a connector {@code fail} failure strategy) must not fail the stream:
     * stopping the consumption is the responsibility of the connector.
     */
    @Test
    void failedNackSkipsMessageAndKeepsConsuming() {
        addBeanClass(FailingNackSource.class, StringSink.class);
        initialize();

        FailingNackSource source = get(FailingNackSource.class);
        await().untilAsserted(() -> {
            assertThat(get(StringSink.class).received()).containsExactly("Luke", "Leia", "Trinity");
            assertThat(source.acks()).isEqualTo(3);
            assertThat(source.nacks()).isEqualTo(2);
        });
    }

    static class PoisonMessage implements Message<String> {
        private final AbstractSource source;

        PoisonMessage(AbstractSource source) {
            this.source = source;
        }

        @Override
        public String getPayload() {
            throw new IllegalStateException("cannot decode");
        }

        @Override
        public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
            return m -> source.ack();
        }

        @Override
        public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
            return (t, m) -> source.nack(t);
        }
    }

    /**
     * Not a bean: the beans below each declare their own {@code @Outgoing} method, as an inherited
     * annotated method would be registered as a second producer.
     */
    abstract static class AbstractSource {
        private final AtomicInteger acks = new AtomicInteger();
        private final List<Throwable> nacks = new CopyOnWriteArrayList<>();

        Multi<Message<String>> messages() {
            return Multi.createFrom().items("Luke", POISON, "Leia", POISON, "Trinity")
                    .map(s -> POISON.equals(s) ? new PoisonMessage(this)
                            : Message.of(s, this::ack, this::nack));
        }

        CompletionStage<Void> ack() {
            acks.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        CompletionStage<Void> nack(Throwable t) {
            nacks.add(t);
            return CompletableFuture.completedFuture(null);
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.size();
        }

        public List<Throwable> nackReasons() {
            return nacks;
        }
    }

    @ApplicationScoped
    public static class Source extends AbstractSource {
        @Outgoing("in")
        public Multi<Message<String>> source() {
            return messages();
        }
    }

    @ApplicationScoped
    public static class FailingNackSource extends AbstractSource {
        @Outgoing("in")
        public Multi<Message<String>> source() {
            return messages();
        }

        @Override
        CompletionStage<Void> nack(Throwable t) {
            super.nack(t);
            return CompletableFuture.failedFuture(new IllegalStateException("nack failed"));
        }
    }

    @ApplicationScoped
    public static class StringSink {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("in")
        public void consume(String s) {
            received.add(s);
        }

        List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class BlockingStringSink {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("in")
        @Blocking
        public void consume(String s) {
            received.add(s);
        }

        List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class MessageSink {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("in")
        public CompletionStage<Void> consume(Message<String> message) {
            received.add(message.getPayload());
            return message.ack();
        }

        List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class PersonProcessor {
        @Incoming("in")
        @Outgoing("out")
        public Person process(Person p) {
            return new Person(p.name.toUpperCase());
        }
    }

    @ApplicationScoped
    public static class PersonSink {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("out")
        public void consume(Person p) {
            received.add(p.name);
        }

        List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    static class StringToPersonConverter implements MessageConverter {
        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return target == Person.class && in.getPayload() instanceof String;
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return in.withPayload(new Person((String) in.getPayload()));
        }
    }

    public static class Person {
        final String name;

        Person(String name) {
            this.name = name;
        }
    }
}
