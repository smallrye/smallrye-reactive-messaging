package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

public class MandatoryMetadataInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingPayload() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingPayload.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingMessage() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingMessage.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
    }

    @Test
    public void testSingleMandatoryMetadataInjectionWhenProcessingMessageAndReturningPayload() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingMessageAndReturningPayload.class);
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
                SingleMandatoryMetadataInjectionWhenProcessingPayload.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingMessage() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingMessage.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().until(() -> source.nacked().size() == 1);
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(5));
        assertThat(source.acked()).hasSize(0);
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                SingleMandatoryMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.nacked()).hasSize(1));
        assertThat(source.acked()).hasSize(0);
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingPayload {

        @Incoming("in")
        @Outgoing("out")
        public String process(String payload, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return payload.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingPayloadBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public String process(String payload, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            return payload.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingMessage {

        @Incoming("in")
        @Outgoing("out")
        public Message<String> process(Message<String> msg, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(metadata).isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
            return msg.withPayload(msg.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingMessageBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public Message<String> process(Message<String> msg, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(metadata).isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
            return msg.withPayload(msg.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class SingleMandatoryMetadataInjectionWhenProcessingMessageAndReturningPayload {

        @Incoming("in")
        @Outgoing("out")
        public String process(Message<String> msg, MetadataInjectionBase.MyMetadata metadata) {
            assertThat(metadata.getId()).isNotZero();
            assertThat(metadata).isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
            assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
            msg.ack();
            return msg.getPayload().toUpperCase();
        }
    }

}
