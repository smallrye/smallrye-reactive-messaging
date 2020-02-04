package org.eclipse.microprofile.reactive.messaging;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public interface MessageBuilder<T> {

    /**
     * This will create a Message referencing the <b>same</b> payload and the <b>same</b> metadata.
     */
    <P> MessageBuilder<T> of(Message<P> message);

    MessageBuilder<T> payload(T Payload);

    MessageBuilder<T> metadata(Iterable<Object> metadata);

    MessageBuilder<T> metadata(Object... metadata);

    MessageBuilder<T> metadata(Metadata metadata);

    MessageBuilder<T> addMetadata(Object... metadata);

    MessageBuilder<T> ack(Supplier<CompletionStage<Void>> ackSupplier);

    Message<T> build();
}
