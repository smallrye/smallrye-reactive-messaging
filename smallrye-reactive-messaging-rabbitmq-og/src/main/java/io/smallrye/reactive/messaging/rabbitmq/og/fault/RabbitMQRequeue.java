package io.smallrye.reactive.messaging.rabbitmq.og.fault;

import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQLogging.log;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQRejectMetadata;
import io.vertx.mutiny.core.Context;

/**
 * Failure handler that requeues the message.
 */
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
        // We mark the message as requeued.
        log.nackedIgnoreMessage(channel);
        log.fullIgnoredFailure(reason);

        // Check if requeue flag is explicitly set in metadata, default to true
        boolean requeue = Optional.ofNullable(metadata)
                .flatMap(md -> md.get(RabbitMQRejectMetadata.class))
                .map(RabbitMQRejectMetadata::isRequeue).orElse(true);

        // Create metadata with requeue flag
        Metadata nackMetadata = metadata != null
                ? metadata.with(new RabbitMQRejectMetadata(requeue))
                : Metadata.of(new RabbitMQRejectMetadata(requeue));

        return msg.nack(reason, nackMetadata);
    }
}
