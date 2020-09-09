package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class MultipleMetadataInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testMultipleMetadataInjectionWhenProcessingPayload() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingPayload.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMultipleMetadataInjectionWhenProcessingMessage() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingMessage.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMultipleMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMultipleMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMultipleMetadataInjectionWhenProcessingMessageAndReturningPayload() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingMessageAndReturningPayload.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayload() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingPayload.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingMessage() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingMessage.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().until(() -> source.nacked().size() == 1);
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                MultipleMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(1));
        assertThat(source.acked()).hasSize(0);
    }

    @ApplicationScoped
    public static class MultipleMetadataInjectionWhenProcessingPayload {

        @Incoming("in")
        @Outgoing("out")
        public String process(String payload, MetadataInjectionBase.MyMetadata metadata,
                Optional<MetadataInjectionBase.MyOtherMetadata> other,
                Optional<UnusedMetadata> unused) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(other).isNotEmpty();
            assertThat(unused).isEmpty();
            return payload.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class MultipleMetadataInjectionWhenProcessingPayloadBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public String process(String payload, MetadataInjectionBase.MyMetadata metadata,
                Optional<MetadataInjectionBase.MyOtherMetadata> other,
                Optional<UnusedMetadata> unused) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(other).isNotEmpty();
            assertThat(unused).isEmpty();
            return payload.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class MultipleMetadataInjectionWhenProcessingMessage {

        @Incoming("in")
        @Outgoing("out")
        public Message<String> process(Message<String> msg, Optional<MetadataInjectionBase.MyOtherMetadata> other,
                MetadataInjectionBase.MyMetadata metadata,
                Optional<UnusedMetadata> unused) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(other).isNotEmpty();
            assertThat(unused).isEmpty();
            assertThat(metadata).isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
            return msg.withPayload(msg.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class MultipleMetadataInjectionWhenProcessingMessageBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public Message<String> process(Message<String> msg, MetadataInjectionBase.MyMetadata metadata,
                Optional<MetadataInjectionBase.MyOtherMetadata> other,
                Optional<UnusedMetadata> unused) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(other).isNotEmpty();
            assertThat(unused).isEmpty();
            assertThat(metadata).isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
            return msg.withPayload(msg.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class MultipleMetadataInjectionWhenProcessingMessageAndReturningPayload {

        @Incoming("in")
        @Outgoing("out")
        public String process(Message<String> msg, MetadataInjectionBase.MyMetadata metadata,
                Optional<MetadataInjectionBase.MyOtherMetadata> other,
                Optional<UnusedMetadata> unused) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(other).isNotEmpty();
            assertThat(unused).isEmpty();
            assertThat(metadata).isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
            msg.ack();
            return msg.getPayload().toUpperCase();
        }
    }

    public static class UnusedMetadata {

    }

}
