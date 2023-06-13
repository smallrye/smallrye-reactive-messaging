package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public class PulsarOutgoingMessage<T> implements PulsarMessage<T>, ContextAwareMessage<T> {

    private final T payload;
    private final PulsarOutgoingMessageMetadata outgoingMessageMetadata;
    private final Metadata metadata;
    private final Supplier<CompletionStage<Void>> ack;
    private final Function<Throwable, CompletionStage<Void>> nack;

    public PulsarOutgoingMessage(T payload,
            Supplier<CompletionStage<Void>> ack,
            Function<Throwable, CompletionStage<Void>> nack) {
        this(payload, ack, nack, PulsarOutgoingMessageMetadata.builder().build());
    }

    public PulsarOutgoingMessage(T payload,
            Supplier<CompletionStage<Void>> ack,
            Function<Throwable, CompletionStage<Void>> nack,
            PulsarOutgoingMessageMetadata outgoingMessageMetadata) {
        this.payload = payload;
        this.ack = ack;
        this.nack = nack;
        this.outgoingMessageMetadata = outgoingMessageMetadata;
        this.metadata = captureContextMetadata(outgoingMessageMetadata);
    }

    public static <T> PulsarOutgoingMessage<T> from(Message<T> message) {
        return new PulsarOutgoingMessage<>(message.getPayload(), message.getAck(), message.getNack(),
                message.getMetadata(PulsarOutgoingMessageMetadata.class)
                        .orElseGet(() -> PulsarOutgoingMessageMetadata.builder().build()));
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public String getKey() {
        return outgoingMessageMetadata.getKey();
    }

    @Override
    public byte[] getKeyBytes() {
        return outgoingMessageMetadata.getKeyBytes();
    }

    @Override
    public boolean hasKey() {
        return outgoingMessageMetadata.hasKey();
    }

    @Override
    public byte[] getOrderingKey() {
        return outgoingMessageMetadata.getOrderingKey();
    }

    @Override
    public Map<String, String> getProperties() {
        return outgoingMessageMetadata.getProperties();
    }

    @Override
    public long getEventTime() {
        return outgoingMessageMetadata.getEventTime();
    }

    @Override
    public long getSequenceId() {
        return outgoingMessageMetadata.getSequenceId();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this.ack;
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this.nack;
    }
}
