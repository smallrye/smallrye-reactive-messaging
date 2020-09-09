package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ProcessorMetadataInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningUni() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class, ProcessorReturningUni.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningUni() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                ProcessorReturningUni.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningCS() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class, ProcessorReturningCS.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningCS() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                ProcessorReturningCS.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningUniOfMessage() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                ProcessorReturningUniOfMessage.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningUniOfMessage() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                ProcessorReturningUniOfMessage.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(1));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningCSOfMessage() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class, ProcessorReturningCSOfMessage.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningCSOfMessage() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                ProcessorReturningCSOfMessage.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(1));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWithProcessorReturningPublisher() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class, ProcessorReturningPublisher.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWithProcessorReturningPublisher() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                ProcessorReturningPublisher.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        // Use pre-processing by default, so we will get 1 ack
        await().untilAsserted(() -> assertThat(source.acked()).hasSize(1));
        assertThat(source.acked()).hasSize(1);
    }

    @ApplicationScoped
    public static class ProcessorReturningUni {

        @Incoming("in")
        @Outgoing("out")
        public Uni<String> process(String payload, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return Uni.createFrom().item(payload.toUpperCase());
        }
    }

    @ApplicationScoped
    public static class ProcessorReturningCS {

        @Incoming("in")
        @Outgoing("out")
        public CompletionStage<String> process(String payload, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return CompletableFuture.completedFuture(payload.toUpperCase());
        }
    }

    @ApplicationScoped
    public static class ProcessorReturningUniOfMessage {

        @Incoming("in")
        @Outgoing("out")
        public Uni<Message<String>> process(Message<String> msg, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return Uni.createFrom().item(msg.withPayload(msg.getPayload().toUpperCase()));
        }
    }

    @ApplicationScoped
    public static class ProcessorReturningCSOfMessage {

        @Incoming("in")
        @Outgoing("out")
        public CompletionStage<Message<String>> process(Message<String> msg, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return CompletableFuture.completedFuture(msg.withPayload(msg.getPayload().toUpperCase()));
        }
    }

    @ApplicationScoped
    public static class ProcessorReturningPublisher {

        @Incoming("in")
        @Outgoing("out")
        public Multi<String> process(String payload, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return Multi.createFrom().item(payload.toUpperCase());
        }
    }

}
