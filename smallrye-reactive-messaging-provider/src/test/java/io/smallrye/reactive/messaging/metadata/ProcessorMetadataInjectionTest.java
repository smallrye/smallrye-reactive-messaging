package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ProcessorMetadataInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningUni() {
        addBeanClass(Source.class, Sink.class, ProcessorReturningUni.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningUni() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, ProcessorReturningUni.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningCS() {
        addBeanClass(Source.class, Sink.class, ProcessorReturningCS.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningCS() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, ProcessorReturningCS.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningUniOfMessage() {
        addBeanClass(Source.class, Sink.class, ProcessorReturningUniOfMessage.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningUniOfMessage() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, ProcessorReturningUniOfMessage.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(1));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningCSOfMessage() {
        addBeanClass(Source.class, Sink.class, ProcessorReturningCSOfMessage.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningCSOfMessage() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, ProcessorReturningCSOfMessage.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(1));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningPublisher() {
        addBeanClass(Source.class, Sink.class, ProcessorReturningPublisher.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningPublisher() {
        addBeanClass(SourceWithoutMyMetadata.class, Sink.class, ProcessorReturningPublisher.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        // Use pre-processing by default, so we will get 1 ack
        await().untilAsserted(() -> assertThat(source.acked()).hasSize(1));
        assertThat(source.acked()).hasSize(1);
    }

    ////////////

    @ApplicationScoped
    public static class ProcessorReturningUni {

        @Incoming("in")
        @Outgoing("out")
        public Uni<String> process(String payload, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return Uni.createFrom().item(payload.toUpperCase());
        }
    }

    @ApplicationScoped
    public static class ProcessorReturningCS {

        @Incoming("in")
        @Outgoing("out")
        public CompletionStage<String> process(String payload, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return CompletableFuture.completedFuture(payload.toUpperCase());
        }
    }

    @ApplicationScoped
    public static class ProcessorReturningUniOfMessage {

        @Incoming("in")
        @Outgoing("out")
        public Uni<Message<String>> process(Message<String> msg, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return Uni.createFrom().item(msg.withPayload(msg.getPayload().toUpperCase()));
        }
    }

    @ApplicationScoped
    public static class ProcessorReturningCSOfMessage {

        @Incoming("in")
        @Outgoing("out")
        public CompletionStage<Message<String>> process(Message<String> msg, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return CompletableFuture.completedFuture(msg.withPayload(msg.getPayload().toUpperCase()));
        }
    }

    @ApplicationScoped
    public static class ProcessorReturningPublisher {

        @Incoming("in")
        @Outgoing("out")
        public Multi<String> process(String payload, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return Multi.createFrom().item(payload.toUpperCase());
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
