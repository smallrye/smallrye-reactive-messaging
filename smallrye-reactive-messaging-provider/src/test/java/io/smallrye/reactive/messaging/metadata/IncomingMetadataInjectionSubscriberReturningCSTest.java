package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Period;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

public class IncomingMetadataInjectionSubscriberReturningCSTest extends WeldTestBaseWithoutTails {

    @ParameterizedTest
    @ValueSource(classes = { SubscriberIngestingPayload.class, SubscriberIngestingPayloadBlocking.class,
            SubscriberIngestingPayloadWithOptional.class, SubscriberIngestingPayloadWithOptionalAndBlocking.class,
            SubscriberIngestingPayloadWithMultipleMetadata.class,
            SubscriberIngestingPayloadWithMultipleMetadataAndBlocking.class })
    void testMetadataInjectingWithSubscriberIngestingPayload(Class<?> subscriber) {
        addBeanClass(Source.class, subscriber);
        initialize();
        verify();
    }

    private void verify() {
        DefaultSink sink = container.select(DefaultSink.class).get();
        Awaitility.await().until(() -> sink.list().size() == 10);

    }

    interface Sink {
        List<String> list();
    }

    public static abstract class DefaultSink implements Sink {
        protected final List<String> list = new CopyOnWriteArrayList<>();

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class SubscriberIngestingPayload extends DefaultSink {

        @Incoming("source")
        public CompletionStage<Void> process(String p, SimplePropagationTest.MsgMetadata metadata) {
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull();
            assertThat(metadata.getMessage()).isEqualTo("foo");
            list.add(p);
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class SubscriberIngestingPayloadBlocking extends DefaultSink {
        @Incoming("source")
        @Blocking
        public CompletionStage<Void> process(String p, SimplePropagationTest.MsgMetadata metadata) {
            assertThat(Infrastructure.canCallerThreadBeBlocked()).isTrue();
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull();
            assertThat(metadata.getMessage()).isEqualTo("foo");
            list.add(p);
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class SubscriberIngestingPayloadWithOptional extends DefaultSink {
        @Incoming("source")
        public CompletionStage<Void> process(String p, Optional<SimplePropagationTest.MsgMetadata> metadata) {
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull().isPresent();
            assertThat(metadata.get().getMessage()).isEqualTo("foo");
            list.add(p);
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class SubscriberIngestingPayloadWithOptionalAndBlocking extends DefaultSink {
        @Incoming("source")

        @Blocking
        public CompletionStage<Void> process(String p, Optional<SimplePropagationTest.MsgMetadata> metadata) {
            assertThat(Infrastructure.canCallerThreadBeBlocked()).isTrue();
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull().isPresent();
            assertThat(metadata.get().getMessage()).isEqualTo("foo");
            list.add(p);
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class SubscriberIngestingPayloadWithMultipleMetadata extends DefaultSink {
        @Incoming("source")

        public CompletionStage<Void> process(String p, Optional<SimplePropagationTest.MsgMetadata> metadata,
                SimplePropagationTest.CounterMetadata counter, Optional<Period> missing, List<String> notThere) {
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull().isPresent();
            assertThat(metadata.get().getMessage()).isEqualTo("foo");
            assertThat(counter).isNotNull();
            assertThat(missing).isEmpty();
            assertThat(notThere).isNull();
            list.add(p);
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class SubscriberIngestingPayloadWithMultipleMetadataAndBlocking extends DefaultSink {
        @Incoming("source")

        @Blocking
        public CompletionStage<Void> process(String p, Optional<SimplePropagationTest.MsgMetadata> metadata,
                SimplePropagationTest.CounterMetadata counter, Optional<Period> missing, List<String> notThere) {
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull().isPresent();
            assertThat(metadata.get().getMessage()).isEqualTo("foo");
            assertThat(counter).isNotNull();
            assertThat(missing).isEmpty();
            assertThat(notThere).isNull();
            list.add(p);
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class Source {

        @Outgoing("source")
        public Multi<Message<String>> source() {
            return Multi.createFrom().range(1, 11)
                    .map(i -> Message.of(Integer.toString(i), Metadata.of(new SimplePropagationTest.CounterMetadata(i),
                            new SimplePropagationTest.MsgMetadata("foo"))));
        }

    }

}
