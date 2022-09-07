package io.smallrye.reactive.messaging.rabbitmq.ack;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.rabbitmq.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging;
import io.vertx.mutiny.core.Context;

/**
 * A {@link RabbitMQAckHandler} used when auto-ack is on.
 */
public class RabbitMQAutoAck implements RabbitMQAckHandler {
    private final String channel;

    /**
     * Constructor.
     *
     * @param channel the channel on which acks are issued
     */
    public RabbitMQAutoAck(String channel) {
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(final IncomingRabbitMQMessage<V> msg, final Context context) {
        RabbitMQLogging.log.ackAutoMessage(channel);
        return ConnectionHolder.runOnContext(context, msg, ignored -> {
        });
    }
}
