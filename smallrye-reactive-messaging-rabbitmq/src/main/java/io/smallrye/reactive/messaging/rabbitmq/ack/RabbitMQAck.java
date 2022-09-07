package io.smallrye.reactive.messaging.rabbitmq.ack;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.rabbitmq.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging;
import io.vertx.mutiny.core.Context;

/**
 * A {@link RabbitMQAckHandler} used when auto-ack is off.
 */
public class RabbitMQAck implements RabbitMQAckHandler {
    private final String channel;

    /**
     * Constructor.
     *
     * @param channel the channel on which acks are issued
     */
    public RabbitMQAck(String channel) {
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(final IncomingRabbitMQMessage<V> msg, final Context context) {
        RabbitMQLogging.log.ackMessage(channel);
        return ConnectionHolder.runOnContext(context, msg, IncomingRabbitMQMessage::acknowledgeMessage);
    }
}
