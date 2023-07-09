package io.smallrye.reactive.messaging.pulsar.fault;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Consumer;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.pulsar.PulsarFailureHandler;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessage;

/**
 * Failure strategy `nack` which calls negative acknowledgement for the message and continues the stream
 */
public class PulsarNack implements PulsarFailureHandler {
    public static final String STRATEGY_NAME = "nack";

    @ApplicationScoped
    @Identifier(STRATEGY_NAME)
    public static class Factory implements PulsarFailureHandler.Factory {

        @Override
        public PulsarFailureHandler create(Consumer<?> consumer, PulsarConnectorIncomingConfiguration config,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new PulsarNack(consumer);
        }
    }

    private final Consumer<?> consumer;

    public PulsarNack(Consumer<?> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Uni<Void> handle(PulsarIncomingMessage<?> message, Throwable reason, Metadata metadata) {
        consumer.negativeAcknowledge(message.getMessageId());
        return Uni.createFrom().voidItem()
                .emitOn(message::runOnMessageContext);
    }
}
