package io.smallrye.reactive.messaging.providers.impl;

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

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;

/**
 * A method consuming a connector message type: on a tracked channel, it receives the messages a converter restores to
 * that type, and the drain waits for them. Without a converter, the messages are negatively acknowledged.
 */
class GracefulShutdownMessageTypeTest extends WeldTestBaseWithoutTails {

    static final List<CustomMessage> RECEIVED = new CopyOnWriteArrayList<>();
    static final List<Throwable> NACKS = new CopyOnWriteArrayList<>();

    @BeforeEach
    void reset() {
        RECEIVED.clear();
        NACKS.clear();
    }

    @AfterAll
    static void cleanup() {
        releaseConfig();
    }

    private void start(Class<?>... beans) {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", CustomMessageConnector.NAME);
        // Longer than the test timeouts, so a drain waiting for its timeout fails the test.
        props.put("mp.messaging.incoming.in.graceful-shutdown.drain-timeout", "30000");
        installConfigFromProperties(props);
        addBeanClass(CustomMessageConnector.class);
        addBeanClass(beans);
        initialize();
    }

    @Test
    void messagesThatNoConverterRestoresAreNegativelyAcknowledged() {
        start(CustomMessageConsumer.class);

        await().atMost(5, TimeUnit.SECONDS).until(() -> NACKS.size() == 3);
        assertThat(RECEIVED).isEmpty();
        assertThat(NACKS).allSatisfy(failure -> assertThat(failure)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SRMSG01011")
                .hasMessageContaining(CustomMessage.class.getName()));

        PausableChannel pausable = get(ChannelRegistry.class).getPausable("in");
        assertThat(((PauserChannel) pausable).getInFlightCount()).isZero();
    }

    @Test
    void methodReceivesTheRestoredMessagesAndTheDrainWaitsForThem() {
        start(CustomMessageConverter.class, CustomMessageConsumer.class);

        await().atMost(5, TimeUnit.SECONDS).until(() -> RECEIVED.size() == 3);
        assertThat(RECEIVED).extracting(CustomMessage::getId).containsExactly(0, 1, 2);
        assertThat(RECEIVED).allSatisfy(message -> assertThat(message).isInstanceOf(RestoredCustomMessage.class));

        PauserChannel pausable = (PauserChannel) get(ChannelRegistry.class).getPausable("in");
        assertThat(pausable.getInFlightCount()).isEqualTo(3);

        CompletableFuture<Void> drain = pausable.pauseAndDrain().subscribeAsCompletionStage();
        assertThat(drain).isNotDone();
        // The restored messages acknowledge through the tracked ones.
        RECEIVED.forEach(message -> message.ack().toCompletableFuture().join());
        await().atMost(2, TimeUnit.SECONDS).until(drain::isDone);
    }

    @Test
    void streamOfConnectorMessagesReceivesTheRestoredMessages() {
        start(CustomMessageConverter.class, CustomMessageStreamConsumer.class);

        // The cast to CustomMessage in the stream would fail on tracked messages.
        await().atMost(5, TimeUnit.SECONDS).until(() -> RECEIVED.size() == 3);
        assertThat(RECEIVED).extracting(CustomMessage::getId).containsExactly(0, 1, 2);
    }

    /**
     * The metadata of the connector messages, from which the converter restores them.
     */
    public record CustomMetadata(int id) {
    }

    /**
     * A connector-specific message type.
     */
    public static class CustomMessage implements Message<String> {

        private final int id;

        CustomMessage(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override
        public String getPayload() {
            return "message-" + id;
        }

        @Override
        public Metadata getMetadata() {
            return Metadata.of(new CustomMetadata(id));
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            NACKS.add(reason);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * A connector message restored from an intercepted one: it delegates to it.
     */
    static class RestoredCustomMessage extends CustomMessage {

        private final Message<?> delegate;

        RestoredCustomMessage(Message<?> delegate) {
            super(delegate.getMetadata(CustomMetadata.class).orElseThrow().id());
            this.delegate = delegate;
        }

        @Override
        public Metadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            return delegate.ack(metadata);
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return delegate.nack(reason, metadata);
        }
    }

    @ApplicationScoped
    public static class CustomMessageConverter implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return target == CustomMessage.class && in.getMetadata(CustomMetadata.class).isPresent();
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return new RestoredCustomMessage(in);
        }
    }

    @ApplicationScoped
    @Connector(CustomMessageConnector.NAME)
    public static class CustomMessageConnector implements InboundConnector {

        static final String NAME = "smallrye-custom-message";

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().range(0, 3).map(CustomMessage::new);
        }
    }

    @ApplicationScoped
    public static class CustomMessageStreamConsumer {

        @Incoming("in")
        @Outgoing("out")
        public Multi<Message<String>> process(Multi<CustomMessage> messages) {
            return messages.map(message -> {
                RECEIVED.add(message);
                return message.withPayload(message.getPayload());
            });
        }

        @Incoming("out")
        public void sink(String payload) {
        }
    }

    @ApplicationScoped
    public static class CustomMessageConsumer {

        @Incoming("in")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public CompletionStage<Void> consume(CustomMessage message) {
            RECEIVED.add(message);
            return CompletableFuture.completedFuture(null);
        }
    }
}
