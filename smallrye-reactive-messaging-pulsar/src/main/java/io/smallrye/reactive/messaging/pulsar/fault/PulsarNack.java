package io.smallrye.reactive.messaging.pulsar.fault;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

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
            return new PulsarNack(consumer, config.getChannel());
        }
    }

    private final Consumer<?> consumer;
    private final String channel;

    public PulsarNack(Consumer<?> consumer, String channel) {
        this.consumer = consumer;
        this.channel = channel;
    }

    @Override
    public Uni<Void> handle(PulsarIncomingMessage<?> message, Throwable reason, Metadata metadata) {
        consumer.negativeAcknowledge(message.getMessageId());
        log.messageFailureNacked(channel, reason.getMessage());
        log.messageFailureFullCause(reason);
        return Uni.createFrom().voidItem()
                .emitOn(message::runOnMessageContext);
    }
}
