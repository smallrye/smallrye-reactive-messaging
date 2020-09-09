package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class SubscriberMetadataInjectTest extends WeldTestBaseWithoutTails {

    @Test
    public void testSubscriberConsumingPayload() {
        addBeanClass(MetadataInjectionBase.Source.class, SubscriberConsumingPayload.class);
        initialize();
        SubscriberConsumingPayload sink = get(SubscriberConsumingPayload.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSubscriberConsumingMessage() {
        addBeanClass(MetadataInjectionBase.Source.class, SubscriberConsumingMessage.class);
        initialize();
        SubscriberConsumingMessage sink = get(SubscriberConsumingMessage.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSubscriberConsumingPayloadReturningUni() {
        addBeanClass(MetadataInjectionBase.Source.class, SubscriberConsumingPayloadReturningUni.class);
        initialize();
        SubscriberConsumingPayloadReturningUni sink = get(SubscriberConsumingPayloadReturningUni.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSubscriberConsumingPayloadWithMissingMandatoryMetadata() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, SubscriberConsumingPayload.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().until(() -> source.nacked().size() == 5);
        assertThat(source.acked()).hasSize(0);
        assertThat(source.nacked()).hasSize(5);
    }

    @Test
    public void testSubscriberConsumingMessageWithMissingMandatoryMetadata() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, SubscriberConsumingMessage.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().until(() -> source.nacked().size() == 1);
        assertThat(source.acked()).hasSize(0);
        assertThat(source.nacked()).hasSize(1);
    }

    @Test
    public void testSubscriberConsumingPayloadReturningUniWithMissingMandatoryMetadata() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, SubscriberConsumingPayloadReturningUni.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().until(() -> source.nacked().size() == 5);
        assertThat(source.acked()).hasSize(0);
        assertThat(source.nacked()).hasSize(5);
    }

    @ApplicationScoped
    public static class SubscriberConsumingPayload {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("in")
        public void consume(String payload, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            list.add(payload);
        }

        public List<String> list() {
            return list;
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @ApplicationScoped
    public static class SubscriberConsumingPayloadReturningUni {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("in")
        public Uni<Void> consume(String payload, MetadataInjectionBase.MyMetadata metadata,
                Optional<MetadataInjectionBase.MyOtherMetadata> other) {
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
        public CompletionStage<Void> consume(Message<String> msg, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            list.add(msg.getPayload());
            return msg.ack();
        }

        public List<String> list() {
            return list;
        }
    }

}
