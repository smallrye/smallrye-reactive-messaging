package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Period;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class IncomingMetadataInjectionProcessorReturningMultiTest extends WeldTestBaseWithoutTails {

    @ParameterizedTest
    @ValueSource(classes = { ProcessorIngestingPayload.class, ProcessorIngestingPayloadWithOptional.class,
            ProcessorIngestingPayloadWithMultipleMetadata.class })
    void testMetadataInjectingInProcessorIngestingPayload() {
        addBeanClass(Source.class, Sink.class, ProcessorIngestingPayload.class);
        initialize();
        IncomingMetadataInjectionProcessorReturningMultiTest.Sink sink = container
                .select(IncomingMetadataInjectionProcessorReturningMultiTest.Sink.class).get();

        assertThat(sink.list()).allSatisfy(message -> {
            SimplePropagationTest.CounterMetadata c = message.getMetadata(SimplePropagationTest.CounterMetadata.class)
                    .orElseThrow(() -> new AssertionError("Metadata expected"));
            SimplePropagationTest.MsgMetadata m = message.getMetadata(SimplePropagationTest.MsgMetadata.class)
                    .orElseThrow(() -> new AssertionError("Metadata expected"));
            assertThat(m.getMessage()).isEqualTo("foo");
            assertThat(c.getCount()).isNotEqualTo(0);
        }).hasSize(10);
    }

    @ApplicationScoped
    public static class ProcessorIngestingPayload {
        @Incoming("source")
        @Outgoing("sink")
        public Multi<String> process(String p, SimplePropagationTest.MsgMetadata metadata) {
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull();
            assertThat(metadata.getMessage()).isEqualTo("foo");
            return Multi.createFrom().item(p);
        }
    }

    @ApplicationScoped
    public static class ProcessorIngestingPayloadWithOptional {
        @Incoming("source")
        @Outgoing("sink")
        public Multi<String> process(String p, Optional<SimplePropagationTest.MsgMetadata> metadata) {
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull().isPresent();
            assertThat(metadata.get().getMessage()).isEqualTo("foo");
            return Multi.createFrom().item(p);
        }
    }

    @ApplicationScoped
    public static class ProcessorIngestingPayloadWithMultipleMetadata {
        @Incoming("source")
        @Outgoing("sink")
        public Multi<String> process(String p, Optional<SimplePropagationTest.MsgMetadata> metadata,
                SimplePropagationTest.CounterMetadata counter, Optional<Period> missing, List<String> notThere) {
            assertThat(p).isNotNull();
            assertThat(metadata).isNotNull().isPresent();
            assertThat(metadata.get().getMessage()).isEqualTo("foo");
            assertThat(counter).isNotNull();
            assertThat(missing).isEmpty();
            assertThat(notThere).isNull();
            return Multi.createFrom().item(p);
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

    @ApplicationScoped
    public static class Sink {
        List<Message<String>> list = new CopyOnWriteArrayList<>();

        @Incoming("sink")
        public CompletionStage<Void> consume(Message<String> message) {
            list.add(message);
            return message.ack();
        }

        public List<Message<String>> list() {
            return list;
        }
    }

}
