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
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class SubscriberMetadataInjectTest extends WeldTestBaseWithoutTails {

    @Test
    public void testSubscriberConsumingPayload() {
        addBeanClass(Source.class, SubscriberConsumingPayload.class);
        initialize();
        SubscriberConsumingPayload sink = get(SubscriberConsumingPayload.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSubscriberConsumingMessage() {
        addBeanClass(Source.class, SubscriberConsumingMessage.class);
        initialize();
        SubscriberConsumingMessage sink = get(SubscriberConsumingMessage.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSubscriberConsumingPayloadReturningUni() {
        addBeanClass(Source.class, SubscriberConsumingPayloadReturningUni.class);
        initialize();
        SubscriberConsumingPayloadReturningUni sink = get(SubscriberConsumingPayloadReturningUni.class);
        Source source = get(Source.class);
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSubscriberConsumingPayloadWithMissingMandatoryMetadata() {
        addBeanClass(SourceWithoutMyMetadata.class, SubscriberConsumingPayload.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().until(() -> source.nacked().size() == 5);
        assertThat(source.acked()).hasSize(0);
        assertThat(source.nacked()).hasSize(5);
    }

    @Test
    public void testSubscriberConsumingMessageWithMissingMandatoryMetadata() {
        addBeanClass(SourceWithoutMyMetadata.class, SubscriberConsumingMessage.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().until(() -> source.nacked().size() == 1);
        assertThat(source.acked()).hasSize(0);
        assertThat(source.nacked()).hasSize(1);
    }

    @Test
    public void testSubscriberConsumingPayloadReturningUniWithMissingMandatoryMetadata() {
        addBeanClass(SourceWithoutMyMetadata.class, SubscriberConsumingPayloadReturningUni.class);
        initialize();
        SourceWithoutMyMetadata source = get(SourceWithoutMyMetadata.class);
        await().until(() -> source.nacked().size() == 5);
        assertThat(source.acked()).hasSize(0);
        assertThat(source.nacked()).hasSize(5);
    }

    @ApplicationScoped
    public static class SubscriberConsumingPayload {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("in")
        public void consume(String payload, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            list.add(payload);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class SubscriberConsumingPayloadReturningUni {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("in")
        public Uni<Void> consume(String payload, MyMetadata metadata, Optional<MyOtherMetadata> other) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(other).isNotEmpty();
            list.add(payload);
            return Uni.createFrom().nullItem();
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class SubscriberConsumingMessage {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("in")
        public CompletionStage<Void> consume(Message<String> msg, MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            list.add(msg.getPayload());
            return msg.ack();
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
