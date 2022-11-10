package io.smallrye.reactive.messaging.pulsar.ack;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Consumer;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarAckHandler;
import io.smallrye.reactive.messaging.pulsar.PulsarConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessage;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactionMetadata;

public class PulsarMessageAck implements PulsarAckHandler {

    public static final String STRATEGY_NAME = "ack";

    @ApplicationScoped
    @Identifier(STRATEGY_NAME)
    public static class Factory implements PulsarAckHandler.Factory {

        @Override
        public PulsarMessageAck create(Consumer<?> consumer, PulsarConnectorIncomingConfiguration config) {
            return new PulsarMessageAck(consumer);
        }
    }

    private final Consumer<?> consumer;

    public PulsarMessageAck(Consumer<?> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Uni<Void> handle(PulsarIncomingMessage<?> message) {
        return Uni.createFrom().completionStage(() -> {
            var txnMetadata = message.getMetadata(PulsarTransactionMetadata.class);
            if (txnMetadata.isPresent()) {
                return consumer.acknowledgeAsync(message.getMessageId(), txnMetadata.get().getTransaction());
            } else {
                return consumer.acknowledgeAsync(message.getMessageId());
            }
        })
                .onFailure().invoke(log::unableToAcknowledgeMessage)
                .emitOn(message::runOnMessageContext);
    }
}
