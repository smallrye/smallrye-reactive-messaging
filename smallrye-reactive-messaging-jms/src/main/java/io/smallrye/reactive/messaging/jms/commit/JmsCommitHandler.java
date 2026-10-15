package io.smallrye.reactive.messaging.jms.commit;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;

/**
 * Jms commit handling strategy, used for message acknowledgment.
 */
@Experimental("Experimental API")
public interface JmsCommitHandler {

    /**
     * Handle message acknowledgment
     *
     * @param message incoming jms message
     * @param metadata associated metadata with acknowledgment
     * @param <T> type of payload
     * @return a completion stage completed when the message acknowledgment has completed.
     */
    <T> Uni<Void> handle(IncomingJmsMessage<T> message, Metadata metadata);

    /**
     * Called on channel shutdown
     */
    default void close() {
        // do nothing by default
    }
}
