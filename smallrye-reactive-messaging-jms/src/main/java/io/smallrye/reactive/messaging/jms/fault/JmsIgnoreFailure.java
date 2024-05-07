package io.smallrye.reactive.messaging.jms.fault;

import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.JmsConnectorIncomingConfiguration;

public class JmsIgnoreFailure implements JmsFailureHandler {

    private final String channel;

    @ApplicationScoped
    @Identifier(Strategy.IGNORE)
    public static class Factory implements JmsFailureHandler.Factory {

        @Override
        public JmsFailureHandler create(JmsConnectorIncomingConfiguration config,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new JmsIgnoreFailure(config.getChannel());
        }
    }

    public JmsIgnoreFailure(String channel) {
        this.channel = channel;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Throwable reason, Metadata metadata) {
        // We commit the message, log and continue
        log.messageNackedIgnore(channel, reason.getMessage());
        log.messageNackedFullIgnored(reason);
        return Uni.createFrom().completionStage(message.ack())
                .emitOn(message::runOnMessageContext);
    }
}
