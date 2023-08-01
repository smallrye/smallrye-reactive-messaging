package io.smallrye.reactive.messaging.rabbitmq.fault;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.rabbitmq.*;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.common.annotation.Identifier;
import io.vertx.mutiny.core.Context;
import org.eclipse.microprofile.reactive.messaging.Metadata;

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
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Metadata metadata, Context context, Throwable reason) {
        // We mark the message as rejected and fail.
        log.nackedIgnoreMessage(channel);
        log.fullIgnoredFailure(reason);
        return ConnectionHolder.runOnContext(context, msg, m -> m.rejectMessage(reason, metadata));
    }
}
