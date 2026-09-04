package io.smallrye.reactive.messaging.rabbitmq.fault;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.rabbitmq.*;
import io.vertx.mutiny.core.Context;

/**
 * A {@link RabbitMQFailureHandler} that rejects the message and reports a failure.
 */
public class RabbitMQFailStop implements RabbitMQFailureHandler {
    private final String channel;
    private final RabbitMQConnector connector;

    @ApplicationScoped
    @Identifier(Strategy.FAIL)
    public static class Factory implements RabbitMQFailureHandler.Factory {

        @Override
        public RabbitMQFailureHandler create(RabbitMQConnectorIncomingConfiguration config, RabbitMQConnector connector) {
            return new RabbitMQFailStop(connector, config.getChannel());
        }
    }

    /**
     * Constructor.
     *
     * @param channel the channel
     */
    public RabbitMQFailStop(RabbitMQConnector connector, String channel) {
        this.connector = connector;
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Metadata metadata, Context context,
            Throwable reason) {
        // We mark the message as rejected and fail.
        log.nackedFailMessage(channel);
        connector.reportIncomingFailure(channel, reason);
        boolean requeue = Optional.ofNullable(metadata)
                .flatMap(md -> md.get(RabbitMQRejectMetadata.class))
                .map(RabbitMQRejectMetadata::isRequeue).orElse(false);
        return ClientHolder.runOnContextAndReportFailure(context, reason, msg, (m) -> m.rejectMessage(reason, requeue));
    }
}
