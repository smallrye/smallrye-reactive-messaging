package io.smallrye.reactive.messaging.jms;

import io.smallrye.common.annotation.Experimental;

/**
 * Transaction mode for the JMS connector incoming channels.
 */
@Experimental("Experimental API")
public enum JmsTransactionMode {

    /**
     * No transaction support. Messages are dispatched to the Vert.x event loop
     * context (default behavior).
     */
    NONE,

    /**
     * Local JMS session transactions. Messages are processed on the poll thread
     * with a Vert.x duplicated context activated via {@code beginDispatch()}.
     * The JMS session is committed after successful processing, rolled back on failure.
     */
    LOCAL,

    /**
     * XA distributed transactions. Messages are processed on the poll thread
     * with a Vert.x duplicated context activated via {@code beginDispatch()}.
     * An XA transaction is started via the {@code TransactionManager}, and the
     * JMS XA resource is enlisted. Other XA resources (e.g., database) can
     * participate in the same distributed transaction.
     */
    XA;

    public static JmsTransactionMode parse(String mode) {
        if (mode == null || mode.isEmpty()) {
            return NONE;
        }
        return valueOf(mode.toUpperCase());
    }
}
