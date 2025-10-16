package io.smallrye.reactive.messaging.kafka.impl;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

/**
 * A wrapper around IncomingKafkaRecord that executes a completion handler after ack/nack.
 * Used for ordered processing to signal when a record has been fully processed.
 */
public class OrderedIncomingKafkaRecord<K, T> extends IncomingKafkaRecord<K, T> {

    private final Runnable postProcessing;

    public OrderedIncomingKafkaRecord(IncomingKafkaRecord<K, T> delegate, Runnable postProcessing) {
        super(delegate);
        this.postProcessing = postProcessing;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        return super.ack(metadata).thenRun(postProcessing);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return super.nack(reason, metadata).thenRun(postProcessing);
    }

}
