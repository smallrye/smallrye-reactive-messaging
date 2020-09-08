package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletableFuture;
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

public class MandatoryMetadataInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingPayload() {
        addBeanClass(Source.class, Sink.class, SingleMandatoryMetadataInjectionWhenProcessingPayload.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingMessage() {
        addBeanClass(Source.class, Sink.class, SingleMandatoryMetadataInjectionWhenProcessingMessage.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(Source.class, Sink.class, SingleMandatoryMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(Source.class, Sink.class, SingleMandatoryMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingMessageAndReturningPayload() {
        addBeanClass(Source.class, Sink.class, SingleMandatoryMetadataInjectionWhenProcessingMessageAndReturningPayload.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayload() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, SingleMandatoryMetadataInjectionWhenProcessingPayload.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingMessage() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, SingleMandatoryMetadataInjectionWhenProcessingMessage.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().until(() -> source.nacked().size() == 1);
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(1));
        assertThat(source.acked()).hasSize(0);
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingPayload {

        @Incoming("in")
        @Outgoing("out")
        public String process(String payload, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return payload.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingPayloadBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public String process(String payload, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return payload.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingMessage {

        @Incoming("in")
        @Outgoing("out")
        public Message<String> process(Message<String> msg, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(metadata).isEqualTo(msg.getMetadata(MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
            return msg.withPayload(msg.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingMessageBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public Message<String> process(Message<String> msg, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(metadata).isEqualTo(msg.getMetadata(MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
            return msg.withPayload(msg.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingMessageAndReturningPayload {

        @Incoming("in")
        @Outgoing("out")
        public String process(Message<String> msg, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(metadata).isEqualTo(msg.getMetadata(MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MyOtherMetadata.class)).isNotEmpty();
            msg.ack();
            return msg.getPayload().toUpperCase();
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
