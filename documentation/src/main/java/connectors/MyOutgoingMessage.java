package connectors;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public class MyOutgoingMessage<T> implements Message<T>, ContextAwareMessage<T> {

    private final T payload;
    private final Metadata metadata;

    private final Supplier<CompletionStage<Void>> ack;
    private final Function<Throwable, CompletionStage<Void>> nack;

    public static <T> MyOutgoingMessage<T> from(Message<T> message) {
        return new MyOutgoingMessage<>(message.getPayload(), message.getMetadata(), message.getAck(), message.getNack());
    }

    public MyOutgoingMessage(T payload, Metadata metadata,
            Supplier<CompletionStage<Void>> ack,
            Function<Throwable, CompletionStage<Void>> nack) {
        this.payload = payload;
        this.metadata = metadata;
        this.ack = ack;
        this.nack = nack;
    }

    public MyOutgoingMessage(T payload, String key, String topic,
            Supplier<CompletionStage<Void>> ack,
            Function<Throwable, CompletionStage<Void>> nack) {
        this(payload, Metadata.of(new MyOutgoingMetadata(topic, key)), ack, nack);
    }

    @Override
    public T getPayload() {
        return payload;
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

    public MyOutgoingMessage<T> withKey(String key) {
        this.metadata.with(this.metadata.get(MyOutgoingMetadata.class)
                .map(m -> MyOutgoingMetadata.builder(m).withKey(key).build()));
        return this;
    }

    public MyOutgoingMessage<T> withTopic(String topic) {
        this.metadata.with(this.metadata.get(MyOutgoingMetadata.class)
                .map(m -> MyOutgoingMetadata.builder(m).withTopic(topic).build()));
        return this;
    }
}
