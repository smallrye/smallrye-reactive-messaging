package io.smallrye.reactive.messaging.rabbitmq.og.ack;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import com.rabbitmq.client.Channel;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQRejectMetadata;
import io.vertx.core.Context;

/**
 * Negative acknowledgement handler for RabbitMQ messages.
 * Calls channel.basicNack() to reject the message with optional requeue.
 */
public class RabbitMQNack implements RabbitMQNackHandler {

    private final Channel channel;
    private final Context context;
    private final boolean defaultRequeue;

    public RabbitMQNack(Channel channel, Context context, boolean defaultRequeue) {
        this.channel = channel;
        this.context = context;
        this.defaultRequeue = defaultRequeue;
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Metadata metadata, Throwable failure) {
        IncomingRabbitMQMetadata rabbitMetadata = message.getRabbitMQMetadata();

        // Get requeue flag from metadata or use default
        boolean requeue = Optional.ofNullable(metadata)
                .flatMap(md -> md.get(RabbitMQRejectMetadata.class))
                .map(RabbitMQRejectMetadata::isRequeue)
                .orElse(defaultRequeue);

        return Uni.createFrom().item(() -> {
            try {
                channel.basicNack(rabbitMetadata.getDeliveryTag(), false, requeue);
                return null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to nack message", e);
            }
        })
                .runSubscriptionOn(command -> context.runOnContext(x -> command.run()))
                .subscribeAsCompletionStage()
                .thenApply(x -> null);
    }

    /**
     * Nack with custom multiple and requeue flags.
     */
    public CompletionStage<Void> nack(long deliveryTag, boolean multiple, boolean requeue) {
        return Uni.createFrom().item(() -> {
            try {
                channel.basicNack(deliveryTag, multiple, requeue);
                return null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to nack message", e);
            }
        })
                .runSubscriptionOn(command -> context.runOnContext(x -> command.run()))
                .subscribeAsCompletionStage()
                .thenApply(x -> null);
    }
}
