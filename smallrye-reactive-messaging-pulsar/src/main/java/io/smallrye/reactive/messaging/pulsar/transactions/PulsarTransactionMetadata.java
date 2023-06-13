package io.smallrye.reactive.messaging.pulsar.transactions;

import org.apache.pulsar.client.api.transaction.Transaction;

public class PulsarTransactionMetadata {

    private final Transaction txn;

    public PulsarTransactionMetadata(Transaction txn) {
        this.txn = txn;
    }

    public Transaction getTransaction() {
        return txn;
    }
}
