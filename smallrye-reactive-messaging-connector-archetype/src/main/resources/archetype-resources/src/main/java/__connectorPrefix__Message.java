package ${package};

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import ${package}.api.ConsumedMessage;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public class ${connectorPrefix}Message<T> implements ContextAwareMessage<T>, MetadataInjectableMessage<T> {

    private final T payload;
    private Metadata metadata;
    private final ${connectorPrefix}AckHandler ackHandler;

    private final ${connectorPrefix}FailureHandler nackHandler;
    private ConsumedMessage<?> consumed;

    public ${connectorPrefix}Message(ConsumedMessage<T> message, ${connectorPrefix}AckHandler ackHandler, ${connectorPrefix}FailureHandler nackHandler) {
        this.consumed = message;
        this.payload = message.body();
        this.ackHandler = ackHandler;
        this.nackHandler = nackHandler;
        this.metadata = ContextAwareMessage.captureContextMetadata(new ${connectorPrefix}IncomingMetadata<>(message));
    }

    public ConsumedMessage<?> getConsumedMessage() {
        return consumed;
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
    public CompletionStage<Void> ack() {
        return ackHandler.handle(this);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata nackMetadata) {
        return nackHandler.handle(this, reason, nackMetadata);
    }

    @Override
    public void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
        }
}
