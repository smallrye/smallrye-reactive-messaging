package io.smallrye.reactive.messaging.jms.fault;

import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.JmsConnector;
import io.smallrye.reactive.messaging.jms.JmsConnectorIncomingConfiguration;

public class JmsFailStop implements JmsFailureHandler {

    private final String channel;
    private final BiConsumer<Throwable, Boolean> reportFailure;

    @ApplicationScoped
    @Identifier(Strategy.FAIL)
    public static class Factory implements JmsFailureHandler.Factory {

        @Override
        public JmsFailureHandler create(JmsConnector jmsConnector, JmsConnectorIncomingConfiguration config,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new JmsFailStop(config.getChannel(), reportFailure);
        }
    }

    public <K, V> JmsFailStop(String channel, BiConsumer<Throwable, Boolean> reportFailure) {
        this.channel = channel;
        this.reportFailure = reportFailure;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Throwable reason, Metadata metadata) {
        // We don't commit, we just fail and stop the client.
        log.messageNackedFailStop(channel);
        // report failure to the connector for health check
        reportFailure.accept(reason, true);
        return Uni.createFrom().<Void> failure(reason)
                .emitOn(message::runOnMessageContext);
    }
}
