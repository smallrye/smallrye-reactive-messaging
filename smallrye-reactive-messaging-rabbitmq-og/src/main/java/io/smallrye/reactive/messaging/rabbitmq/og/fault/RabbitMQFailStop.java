package io.smallrye.reactive.messaging.rabbitmq.og.fault;

import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQLogging.log;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnectorIncomingConfiguration;
import io.vertx.mutiny.core.Context;

/**
 * Failure handler that fails (stops) on error.
 */
public class RabbitMQFailStop implements RabbitMQFailureHandler {

    private final String channel;

    @ApplicationScoped
    @Identifier(Strategy.FAIL)
    public static class Factory implements RabbitMQFailureHandler.Factory {

        @Override
        public RabbitMQFailureHandler create(RabbitMQConnectorIncomingConfiguration config, RabbitMQConnector connector) {
            return new RabbitMQFailStop(config.getChannel());
        }
    }

    /**
     * Constructor.
     *
     * @param channel the channel
     */
    public RabbitMQFailStop(String channel) {
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Metadata metadata, Context context,
            Throwable reason) {
        log.nackedFailMessage(channel);
        return Uni.createFrom().completionStage(msg.nack(reason, metadata))
                .onItem().transformToUni(v -> Uni.createFrom().<Void> failure(reason))
                .subscribeAsCompletionStage();
    }
}
