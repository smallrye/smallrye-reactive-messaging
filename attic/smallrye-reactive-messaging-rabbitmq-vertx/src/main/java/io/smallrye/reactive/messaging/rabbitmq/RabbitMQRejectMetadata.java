package io.smallrye.reactive.messaging.rabbitmq;

import org.eclipse.microprofile.reactive.messaging.Metadata;

/**
 * Additional nack metadata that specifies flags for the 'basic.reject' operation.
 *
 * @see {@link IncomingRabbitMQMessage#nack(Throwable, Metadata)}
 */
public class RabbitMQRejectMetadata {

    private final boolean requeue;

    /**
     * Constructor.
     *
     * @param requeue requeue the message
     */
    public RabbitMQRejectMetadata(boolean requeue) {
        this.requeue = requeue;
    }

    /**
     * If requeue is true, the server will attempt to requeue the message.
     * If requeue is false or the requeue attempt fails the messages are discarded or dead-lettered.
     *
     * @return requeue the message
     */
    public boolean isRequeue() {
        return requeue;
    }
}
