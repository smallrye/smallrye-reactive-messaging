package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class OptionalMetadataInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingPayload() {
        addBeanClass(Source.class, Sink.class, SingleOptionalMetadataInjectionWhenProcessingPayload.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("found", "found", "found", "found", "found");
    }

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingMessage() {
        addBeanClass(Source.class, Sink.class, SingleOptionalMetadataInjectionWhenProcessingMessage.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO-OK", "HELLO-OK", "HELLO-OK", "HELLO-OK", "HELLO-OK");
    }

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(Source.class, Sink.class, SingleOptionalMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("found", "found", "found", "found", "found");
    }

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(Source.class, Sink.class, SingleOptionalMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("found", "found", "found", "found", "found");
    }

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingMessageAndReturningPayload() {
        addBeanClass(Source.class, Sink.class, SingleOptionalMetadataInjectionWhenProcessingMessageAndReturningPayload.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO-OK", "HELLO-OK", "HELLO-OK", "HELLO-OK", "HELLO-OK");
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayload() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, SingleOptionalMetadataInjectionWhenProcessingPayload.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        Sink sink = get(Sink.class);
        await().untilAsserted(() -> assertThat(source.acked()).hasSize(5));
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO", "HELLO", "HELLO", "HELLO", "HELLO");
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingMessage() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, SingleOptionalMetadataInjectionWhenProcessingMessage.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().until(() -> source.acked().size() == 5);
        Sink sink = get(Sink.class);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO", "HELLO", "HELLO", "HELLO", "HELLO");
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.acked()).hasSize(5));
        Sink sink = get(Sink.class);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO", "HELLO", "HELLO", "HELLO", "HELLO");
    }

    @Test
    public void testMissingOptionalMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.acked()).hasSize(5));
        Sink sink = get(Sink.class);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO", "HELLO", "HELLO", "HELLO", "HELLO");
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingPayload {

        @Incoming("in")
        @Outgoing("out")
        public String process(String payload, Optional<MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata.orElse(new MyMetadata(0)).getId()).isNotZero();
                return "found";
            } else {
                return payload.toUpperCase();
            }

        }
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingPayloadBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public String process(String payload, Optional<MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata.orElse(new MyMetadata(0)).getId()).isNotZero();
                return "found";
            }
            return payload.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingMessage {

        @Incoming("in")
        @Outgoing("out")
        public Message<String> process(Message<String> msg, Optional<MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata).isNotEmpty();
                assertThat(metadata.orElse(new MyMetadata(0)).getId()).isNotZero();
                assertThat(metadata.orElse(null)).isEqualTo(msg.getMetadata(MyMetadata.class).orElse(null));
                assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
                return msg.withPayload(msg.getPayload().toUpperCase() + "-OK");
            } else {
                assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
                return msg.withPayload(msg.getPayload().toUpperCase());
            }
        }
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingMessageBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public Message<String> process(Message<String> msg, Optional<MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata.orElse(new MyMetadata(0)).getId()).isNotZero();
                assertThat(metadata.orElse(null)).isEqualTo(msg.getMetadata(MyMetadata.class).orElse(null));
                assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
                return msg.withPayload("found");
            }
            assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
            return msg.withPayload(msg.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingMessageAndReturningPayload {

        @Incoming("in")
        @Outgoing("out")
        public CompletionStage<String> process(Message<String> msg, Optional<MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata.orElse(new MyMetadata(0)).getId()).isNotZero();
                assertThat(metadata.orElse(null)).isEqualTo(msg.getMetadata(MyMetadata.class).orElse(null));
                assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
                return msg.ack()
                        .thenApply(x -> msg.getPayload().toUpperCase() + "-OK");
            } else {
                assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
                return msg.ack()
                        .thenApply(x -> msg.getPayload().toUpperCase());
            }
        }
    }

    @ApplicationScoped
    public static class Sink {
        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("out")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class Source {
        int i = 0;
        List<Integer> acked = new CopyOnWriteArrayList<>();
        List<Integer> nacked = new CopyOnWriteArrayList<>();

        @Outgoing("in")
        public Multi<Message<String>> producer() {
            return Multi.createFrom().range(0, 5)
                    .emitOn(Infrastructure.getDefaultExecutor())
                    .onItem().transform(i -> {
                        i = i + 1;
                        int v = i;
                        return Message.of("hello")
                                .addMetadata(new MyMetadata(i))
                                .addMetadata(new MyOtherMetadata(Integer.toString(i)))
                                .withAck(() -> {
                                    acked.add(v);
                                    return CompletableFuture.completedFuture(null);
                                })
                                .withNack(t -> {
                                    nacked.add(v);
                                    return CompletableFuture.completedFuture(null);
                                });

                    });
        }

        public List<Integer> acked() {
            return acked;
        }

        public List<Integer> nacked() {
            return nacked;
        }
    }

    @ApplicationScoped
    public static class SourceWithoutMyMetadata {
        int i = 0;
        List<Integer> acked = new CopyOnWriteArrayList<>();
        List<Integer> nacked = new CopyOnWriteArrayList<>();

        @Outgoing("in")
        public Multi<Message<String>> producer() {
            return Multi.createFrom().range(0, 5)
                    .emitOn(Infrastructure.getDefaultExecutor())
                    .onItem().transform(i -> {
                        i = i + 1;
                        int v = i;
                        return Message.of("hello")
                                .addMetadata(new MyOtherMetadata(Integer.toString(i)))
                                .withAck(() -> {
                                    acked.add(v);
                                    return CompletableFuture.completedFuture(null);
                                })
                                .withNack(t -> {
                                    nacked.add(v);
                                    return CompletableFuture.completedFuture(null);
                                });

                    });
        }

        public List<Integer> acked() {
            return acked;
        }

        public List<Integer> nacked() {
            return nacked;
        }
    }

    public static class MyMetadata {
        private final int id;

        public MyMetadata(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }

    public static class MyOtherMetadata {
        private final String id;

        public MyOtherMetadata(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

}
