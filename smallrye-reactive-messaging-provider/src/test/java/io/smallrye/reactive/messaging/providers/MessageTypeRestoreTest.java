package io.smallrye.reactive.messaging.providers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;

/**
 * A method consuming a specific message type gets it restored by a converter when the message lost it on the way
 * (here, a decorator rebuilding messages as interception does).
 */
class MessageTypeRestoreTest extends WeldTestBaseWithoutTails {

    @AfterEach
    void cleanup() {
        releaseConfig();
    }

    @Test
    void converterRestoresTheConsumedMessageType() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.typed.connector", TypedConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(TypedConnector.class, RebuildingDecorator.class, TypedMessageConverter.class, TypedConsumer.class);
        initialize();

        TypedConsumer consumer = get(TypedConsumer.class);
        await().atMost(5, TimeUnit.SECONDS).until(() -> consumer.received().size() == 3);

        assertThat(consumer.received()).allSatisfy(m -> assertThat(m).isInstanceOf(TypedMessage.class));
        assertThat(consumer.received()).extracting(Message::getPayload).containsExactly("a", "b", "c");
        // Acknowledgement goes through the rebuilt message down to the connector's message.
        await().untilAsserted(() -> assertThat(TypedConnector.ACKS).hasValue(3));
    }

    public record TypedMetadata(String origin) {
    }

    /**
     * A connector-specific message type.
     */
    public static class TypedMessage implements Message<String> {

        private final String payload;
        private final Metadata metadata;
        private final Function<Metadata, CompletionStage<Void>> ack;

        TypedMessage(String payload, Metadata metadata, Function<Metadata, CompletionStage<Void>> ack) {
            this.payload = payload;
            this.metadata = metadata;
            this.ack = ack;
        }

        @Override
        public String getPayload() {
            return payload;
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }

        @Override
        public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
            return ack;
        }
    }

    @ApplicationScoped
    @Connector(TypedConnector.NAME)
    public static class TypedConnector implements InboundConnector {

        static final String NAME = "smallrye-typed";
        static final AtomicInteger ACKS = new AtomicInteger();

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            ACKS.set(0);
            return Multi.createFrom().items("a", "b", "c")
                    .map(p -> new TypedMessage(p, Metadata.of(new TypedMetadata("typed")), m -> {
                        ACKS.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    }));
        }
    }

    /**
     * Rebuilds every message, keeping payload, metadata and acknowledgement but not the message type.
     */
    @ApplicationScoped
    public static class RebuildingDecorator implements PublisherDecorator {

        @Override
        public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
                Config channelConfig) {
            return publisher.map(m -> m.withAckWithMetadata(m::ack));
        }
    }

    @ApplicationScoped
    public static class TypedMessageConverter implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return target == TypedMessage.class && !(in instanceof TypedMessage)
                    && in.getMetadata(TypedMetadata.class).isPresent();
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return new TypedMessage((String) in.getPayload(), in.getMetadata(), in::ack);
        }
    }

    @ApplicationScoped
    public static class TypedConsumer {

        private final List<Message<String>> received = new CopyOnWriteArrayList<>();

        @Incoming("typed")
        public CompletionStage<Void> consume(TypedMessage message) {
            received.add(message);
            return message.ack();
        }

        public List<Message<String>> received() {
            return received;
        }
    }
}
