package io.smallrye.reactive.messaging.rabbitmq.og.ack;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import com.rabbitmq.client.Channel;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata;
import io.vertx.core.Context;

/**
 * Manual acknowledgement handler for RabbitMQ messages.
 * Calls channel.basicAck() to acknowledge the message.
 */
public class RabbitMQAck implements RabbitMQAckHandler {

    private final Channel channel;
    private final Context context;

    public RabbitMQAck(Channel channel, Context context) {
        this.channel = channel;
        this.context = context;
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message) {
        IncomingRabbitMQMetadata metadata = message.getRabbitMQMetadata();
        return Uni.createFrom().item(() -> {
            try {
                channel.basicAck(metadata.getDeliveryTag(), false);
                return null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to acknowledge message", e);
            }
        })
                .runSubscriptionOn(command -> context.runOnContext(x -> command.run()))
                .subscribeAsCompletionStage()
                .thenApply(x -> null);
    }

    /**
     * Acknowledge with multiple flag.
     */
    public CompletionStage<Void> ack(long deliveryTag, boolean multiple) {
        return Uni.createFrom().item(() -> {
            try {
                channel.basicAck(deliveryTag, multiple);
                return null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to acknowledge message", e);
            }
        })
                .runSubscriptionOn(command -> context.runOnContext(x -> command.run()))
                .subscribeAsCompletionStage()
                .thenApply(x -> null);
    }
}
