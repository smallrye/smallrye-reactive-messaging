package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;
import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarMessages.msg;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactionMetadata;

public class PulsarIncomingBatchMessage<T> implements PulsarBatchMessage<T>, MetadataInjectableMessage<List<T>> {
    private final Messages<T> delegate;
    private final List<T> payload;
    private Metadata metadata;
    private final List<PulsarMessage<T>> incomingMessages;

    public PulsarIncomingBatchMessage(Messages<T> messages, PulsarAckHandler ackHandler, PulsarFailureHandler nackHandler) {
        this.delegate = Objects.requireNonNull(messages, msg.isRequired("messages"));
        Objects.requireNonNull(ackHandler, msg.isRequired("ack"));
        Objects.requireNonNull(nackHandler, msg.isRequired("nack"));
        List<PulsarIncomingMessage<T>> incomings = new ArrayList<>();
        List<T> payloads = new ArrayList<>();
        for (Message<T> message : messages) {
            incomings.add(new PulsarIncomingMessage<>(message, ackHandler, nackHandler));
            payloads.add(message.getValue());
        }
        this.incomingMessages = Collections.unmodifiableList(incomings);
        this.payload = Collections.unmodifiableList(payloads);
        List<PulsarIncomingMessage<?>> batchedMessages = (List<PulsarIncomingMessage<?>>) (List) incomings;
        this.metadata = captureContextMetadata(new PulsarIncomingBatchMessageMetadata(messages, batchedMessages));
    }

    /**
     * Creates a batch message from the given messages, payload and metadata. Acknowledging it acknowledges the given
     * messages, unless the subclass overrides {@link #ack(Metadata)} and {@link #nack(Throwable, Metadata)}.
     *
     * @param messages the Pulsar messages
     * @param incomingMessages the incoming messages of the batch
     * @param payload the payload of the batch
     * @param metadata the metadata of the batch
     */
    PulsarIncomingBatchMessage(Messages<T> messages, List<PulsarMessage<T>> incomingMessages, List<T> payload,
            Metadata metadata) {
        this.delegate = Objects.requireNonNull(messages, msg.isRequired("messages"));
        this.incomingMessages = Collections.unmodifiableList(incomingMessages);
        this.payload = payload;
        this.metadata = Objects.requireNonNull(metadata, msg.isRequired("metadata"));
    }

    @Override
    public List<T> getPayload() {
        return this.payload;
    }

    public Messages<T> unwrap() {
        return delegate;
    }

    @Override
    public List<PulsarMessage<T>> getMessages() {
        return incomingMessages;
    }

    @Override
    public Iterator<PulsarMessage<T>> iterator() {
        return incomingMessages.iterator();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        return Multi.createFrom().iterable(incomingMessages)
                .plug(stream -> {
                    var txnMetadata = getMetadata(PulsarTransactionMetadata.class);
                    if (txnMetadata.isPresent()) {
                        return stream.onItem().invoke(m -> ((PulsarIncomingMessage) m).injectMetadata(txnMetadata.get()));
                    } else {
                        return stream;
                    }
                })
                .onItem().transformToUniAndMerge(m -> Uni.createFrom().completionStage(m.getAckWithMetadata().apply(metadata)))
                .toUni().subscribeAsCompletionStage();
    }

    @Override
    public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        return this::ack;
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return Multi.createFrom().iterable(incomingMessages)
                .onItem().transformToUniAndMerge(m -> Uni.createFrom().completionStage(() -> m.nack(reason, metadata)))
                .toUni().subscribeAsCompletionStage();
    }

    @Override
    public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
        return this::nack;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarIncomingBatchMessage<?> that = (PulsarIncomingBatchMessage<?>) o;
        return delegate.equals(that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate);
    }

    @Override
    public synchronized void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }
}
