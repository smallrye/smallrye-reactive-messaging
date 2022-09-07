package io.smallrye.reactive.messaging.amqp.fault;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.amqp.ConnectionHolder;
import io.vertx.mutiny.core.Context;

/**
 * This nack strategy marking the message as {@code modified} and set the {@code delivery-failed} attribute to {@code true},
 * as well as the {@code undeliverable-here} flag to {@code true}.
 * <p>
 * The message will not be redelivered on the same node, but may be redelivered on another node.
 * <p>
 * See http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified.
 */
public class AmqpModifiedFailedAndUndeliverableHere implements AmqpFailureHandler {

    private final String channel;

    public AmqpModifiedFailedAndUndeliverableHere(String channel) {
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(AmqpMessage<V> msg, Context context, Throwable reason) {
        log.nackedModifiedFailedMessageAndUndeliverableHere(channel);
        log.fullIgnoredFailure(reason);
        return ConnectionHolder.runOnContext(context, msg, m -> m.modified(true, true));
    }
}
