package io.smallrye.reactive.messaging.rabbitmq.fault;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQRejectMetadata;
import io.vertx.mutiny.core.Context;

/**
 * Failure handler that rejects the message without requeuing.
 */
public class RabbitMQReject implements RabbitMQFailureHandler {

    private final String channel;

    @ApplicationScoped
    @Identifier(Strategy.REJECT)
    public static class Factory implements RabbitMQFailureHandler.Factory {

        @Override
        public RabbitMQFailureHandler create(RabbitMQConnectorIncomingConfiguration config, RabbitMQConnector connector) {
            return new RabbitMQReject(config.getChannel());
        }
    }

    /**
     * Constructor.
     *
     * @param channel the channel
     */
    public RabbitMQReject(String channel) {
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Metadata metadata, Context context,
            Throwable reason) {
        // We mark the message as rejected without requeue.
        log.nackedIgnoreMessage(channel);
        log.fullIgnoredFailure(reason);

        // Create metadata with requeue=false
        Metadata nackMetadata = metadata != null
                ? metadata.with(new RabbitMQRejectMetadata(false))
                : Metadata.of(new RabbitMQRejectMetadata(false));

        return msg.nack(reason, nackMetadata);
    }
}
