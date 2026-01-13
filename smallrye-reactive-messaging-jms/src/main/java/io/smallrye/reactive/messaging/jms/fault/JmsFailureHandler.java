package io.smallrye.reactive.messaging.jms.fault;

import java.util.function.BiConsumer;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.JmsConnector;
import io.smallrye.reactive.messaging.jms.JmsConnectorIncomingConfiguration;

/**
 * Jms Failure handling strategy
 */
@Experimental("Experimental API")
public interface JmsFailureHandler {
    /**
     * Identifiers of default failure strategies
     */
    interface Strategy {
        String FAIL = "fail";
        String IGNORE = "ignore";
        String DEAD_LETTER_QUEUE = "dead-letter-queue";

    }

    /**
     * Factory interface for {@link JmsFailureHandler}
     */
    interface Factory {
        JmsFailureHandler create(
                JmsConnector connector,
                JmsConnectorIncomingConfiguration config,
                BiConsumer<Throwable, Boolean> reportFailure);
    }

    /**
     * Handle message negative-acknowledgment
     *
     * @param record incoming jms message
     * @param reason nack reason
     * @param metadata associated metadata with negative-acknowledgment
     * @param <T> type of payload
     * @return a completion stage completed when the message is negative-acknowledgement has completed.
     */
    <T> Uni<Void> handle(IncomingJmsMessage<T> record, Throwable reason, Metadata metadata);

    /**
     * Called on channel shutdown
     */
    default void close() {
        // do nothing by default
    }
}
