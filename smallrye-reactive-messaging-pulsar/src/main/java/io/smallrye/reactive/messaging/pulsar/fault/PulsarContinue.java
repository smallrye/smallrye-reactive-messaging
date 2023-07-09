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
 * Failure strategy `continue` which calls logs message failure but continues the stream without nacking or acking the message
 */
public class PulsarContinue implements PulsarFailureHandler {
    public static final String STRATEGY_NAME = "continue";

    @ApplicationScoped
    @Identifier(STRATEGY_NAME)
    public static class Factory implements PulsarFailureHandler.Factory {

        @Override
        public PulsarFailureHandler create(Consumer<?> consumer, PulsarConnectorIncomingConfiguration config,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new PulsarContinue(config.getChannel());
        }
    }

    private final String channel;

    public PulsarContinue(String channel) {
        this.channel = channel;
    }

    @Override
    public Uni<Void> handle(PulsarIncomingMessage<?> message, Throwable reason, Metadata metadata) {
        log.messageNackedIgnored(channel, reason.getMessage());
        log.messageNackedFullIgnored(reason);
        return Uni.createFrom().voidItem()
                .emitOn(message::runOnMessageContext);
    }
}
