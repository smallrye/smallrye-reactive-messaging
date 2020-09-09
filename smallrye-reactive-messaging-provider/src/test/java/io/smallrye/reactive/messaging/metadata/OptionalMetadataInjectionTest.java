package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class OptionalMetadataInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingPayload() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingPayload.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("found", "found", "found", "found", "found");
    }

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingMessage() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingMessage.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO-OK", "HELLO-OK", "HELLO-OK", "HELLO-OK", "HELLO-OK");
    }

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("found", "found", "found", "found", "found");
    }

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("found", "found", "found", "found", "found");
    }

    @Test
    public void testSingleOptionalMetadataInjectionWhenProcessingMessageAndReturningPayload() {
        addBeanClass(MetadataInjectionBase.Source.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingMessageAndReturningPayload.class);
        initialize();
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        MetadataInjectionBase.Source source = get(MetadataInjectionBase.Source.class);
        source.run();
        await().until(() -> sink.list().size() == 5);
        assertThat(source.acked()).hasSize(5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO-OK", "HELLO-OK", "HELLO-OK", "HELLO-OK", "HELLO-OK");
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayload() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingPayload.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.acked()).hasSize(5));
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO", "HELLO", "HELLO", "HELLO", "HELLO");
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingMessage() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingMessage.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        source.run();
        await().until(() -> source.acked().size() == 5);
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO", "HELLO", "HELLO", "HELLO", "HELLO");
    }

    @Test
    public void testMissingMandatoryMetadataInjectionWhenProcessingPayloadBlocking() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingPayloadBlocking.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.acked()).hasSize(5));
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO", "HELLO", "HELLO", "HELLO", "HELLO");
    }

    @Test
    public void testMissingOptionalMetadataInjectionWhenProcessingMessageBlocking() {
        addBeanClass(MetadataInjectionBase.SourceWithoutMyMetadata.class, MetadataInjectionBase.Sink.class,
                SingleOptionalMetadataInjectionWhenProcessingMessageBlocking.class);
        initialize();
        MetadataInjectionBase.SourceWithoutMyMetadata source = get(MetadataInjectionBase.SourceWithoutMyMetadata.class);
        MetadataInjectionBase.Sink sink = get(MetadataInjectionBase.Sink.class);
        source.run();
        await().untilAsserted(() -> assertThat(source.acked()).hasSize(5));
        assertThat(source.nacked()).hasSize(0);
        assertThat(sink.list()).containsExactly("HELLO", "HELLO", "HELLO", "HELLO", "HELLO");
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingPayload {

        @Incoming("in")
        @Outgoing("out")
        public String process(String payload, Optional<MetadataInjectionBase.MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata.orElse(new MetadataInjectionBase.MyMetadata(0)).getId()).isNotZero();
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
        public String process(String payload, Optional<MetadataInjectionBase.MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata.orElse(new MetadataInjectionBase.MyMetadata(0)).getId()).isNotZero();
                return "found";
            }
            return payload.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingMessage {

        @Incoming("in")
        @Outgoing("out")
        public Message<String> process(Message<String> msg, Optional<MetadataInjectionBase.MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata).isNotEmpty();
                assertThat(metadata.orElse(new MetadataInjectionBase.MyMetadata(0)).getId()).isNotZero();
                assertThat(metadata.orElse(null))
                        .isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
                assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
                return msg.withPayload(msg.getPayload().toUpperCase() + "-OK");
            } else {
                assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
                return msg.withPayload(msg.getPayload().toUpperCase());
            }
        }
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingMessageBlocking {

        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public Message<String> process(Message<String> msg, Optional<MetadataInjectionBase.MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata.orElse(new MetadataInjectionBase.MyMetadata(0)).getId()).isNotZero();
                assertThat(metadata.orElse(null))
                        .isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
                assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
                return msg.withPayload("found");
            }
            assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
            return msg.withPayload(msg.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class SingleOptionalMetadataInjectionWhenProcessingMessageAndReturningPayload {

        @Incoming("in")
        @Outgoing("out")
        public CompletionStage<String> process(Message<String> msg,
                Optional<MetadataInjectionBase.MyMetadata> metadata) {
            if (metadata.isPresent()) {
                assertThat(metadata.orElse(new MetadataInjectionBase.MyMetadata(0)).getId()).isNotZero();
                assertThat(metadata.orElse(null))
                        .isEqualTo(msg.getMetadata(MetadataInjectionBase.MyMetadata.class).orElse(null));
                assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
                return msg.ack()
                        .thenApply(x -> msg.getPayload().toUpperCase() + "-OK");
            } else {
                assertThat(msg.getMetadata(MetadataInjectionBase.MyOtherMetadata.class)).isNotEmpty();
                return msg.ack()
                        .thenApply(x -> msg.getPayload().toUpperCase());
            }
        }
    }

}
