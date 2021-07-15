package io.smallrye.reactive.messaging.rabbitmq.fault;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.rabbitmq.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging;
import io.vertx.mutiny.core.Context;

/**
 * A {@link RabbitMQFailureHandler} that rejects the message and reports a failure.
 */
public class RabbitMQFailStop implements RabbitMQFailureHandler {
    private final String channel;
    private final RabbitMQConnector connector;

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
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Context context, Throwable reason) {
        // We mark the message as rejected and fail.
        RabbitMQLogging.log.nackedFailMessage(channel);
        connector.reportIncomingFailure(channel, reason);
        return ConnectionHolder.runOnContextAndReportFailure(context, reason, () -> msg.rejectMessage(reason));
    }
}
