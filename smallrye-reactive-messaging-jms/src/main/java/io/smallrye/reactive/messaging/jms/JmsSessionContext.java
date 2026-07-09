package io.smallrye.reactive.messaging.jms;

import jakarta.jms.JMSContext;

import io.smallrye.common.annotation.Experimental;

/**
 * Message metadata that carries the JMS session context from the incoming
 * source to the outgoing sink. When present, the sink uses this context
 * for sending instead of creating its own, ensuring both the receive and
 * send participate in the same transaction.
 */
@Experimental("Experimental API")
public record JmsSessionContext(JMSContext jmsContext, JmsTransactionMode transactionMode) {

}
