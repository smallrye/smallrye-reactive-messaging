package io.smallrye.reactive.messaging.rabbitmq.fault;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.rabbitmq.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQRejectMetadata;
import io.vertx.mutiny.core.Context;

public class RabbitMQRequeue implements RabbitMQFailureHandler {
    private final String channel;

    @ApplicationScoped
    @Identifier(Strategy.REQUEUE)
    public static class Factory implements RabbitMQFailureHandler.Factory {

        @Override
        public RabbitMQFailureHandler create(RabbitMQConnectorIncomingConfiguration config, RabbitMQConnector connector) {
            return new RabbitMQRequeue(config.getChannel());
        }
    }

    /**
     * Constructor.
     *
     * @param channel the channel
     */
    public RabbitMQRequeue(String channel) {
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Metadata metadata, Context context,
            Throwable reason) {
        // We mark the message as requeued and fail.
        log.nackedIgnoreMessage(channel);
        log.fullIgnoredFailure(reason);
        boolean requeue = Optional.ofNullable(metadata)
                .flatMap(md -> md.get(RabbitMQRejectMetadata.class))
                .map(RabbitMQRejectMetadata::isRequeue).orElse(true);
        return ConnectionHolder.runOnContext(context, msg, m -> m.rejectMessage(reason, requeue));
    }
}
