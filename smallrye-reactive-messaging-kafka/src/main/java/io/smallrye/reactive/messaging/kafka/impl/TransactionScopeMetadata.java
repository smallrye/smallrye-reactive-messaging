package io.smallrye.reactive.messaging.kafka.impl;

import io.smallrye.reactive.messaging.kafka.KafkaProducer;

/**
 * Message metadata that carries a reference to the {@link KafkaProducer} transaction scope
 * through which the send should be routed. This ensures that the send goes through the
 * scope's lazy transaction management rather than directly to the top-level producer.
 * <p>
 * Used by pooled transaction mode: {@code KafkaTransactionsImpl.Transaction} attaches
 * this metadata, and {@code KafkaSink} reads it to route the send through the scope.
 */
public class TransactionScopeMetadata {

    private final KafkaProducer<?, ?> scope;

    public TransactionScopeMetadata(KafkaProducer<?, ?> scope) {
        this.scope = scope;
    }

    public KafkaProducer<?, ?> getScope() {
        return scope;
    }
}
