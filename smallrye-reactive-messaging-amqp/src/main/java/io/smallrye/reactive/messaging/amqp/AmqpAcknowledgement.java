package io.smallrye.reactive.messaging.amqp;

import java.util.function.Consumer;

import io.smallrye.common.annotation.Experimental;
import io.vertx.mutiny.amqp.AmqpMessage;

/**
 * Mutable metadata attached to incoming AMQP messages.
 * Processing code can inject this to control the AMQP disposition
 * applied when the message is acked or nacked.
 * <p>
 * If no disposition is set, ack defaults to {@code accepted} and
 * nack uses the channel's configured failure strategy.
 */
@Experimental("Experimental API")
public class AmqpAcknowledgement {

    private volatile Consumer<AmqpMessage> disposition;
    private volatile boolean fatal;

    public AmqpAcknowledgement accepted() {
        this.disposition = AmqpMessage::accepted;
        return this;
    }

    public AmqpAcknowledgement rejected() {
        this.disposition = AmqpMessage::rejected;
        return this;
    }

    public AmqpAcknowledgement released() {
        this.disposition = AmqpMessage::released;
        return this;
    }

    public AmqpAcknowledgement modifiedFailed() {
        this.disposition = m -> m.modified(true, false);
        return this;
    }

    public AmqpAcknowledgement modifiedFailedUndeliverableHere() {
        this.disposition = m -> m.modified(true, true);
        return this;
    }

    public AmqpAcknowledgement fatal() {
        this.fatal = true;
        return this;
    }

    public Consumer<AmqpMessage> getDisposition() {
        return disposition;
    }

    public boolean isFatal() {
        return fatal;
    }
}
