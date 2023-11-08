package connectors;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import connectors.api.ConsumedMessage;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public class MyInjectableMessage<T> implements ContextAwareMessage<T>, MetadataInjectableMessage<T> {

    private final T payload;
    Metadata metadata;

    public MyInjectableMessage(ConsumedMessage<T> message) {
        this.payload = message.body();
        this.metadata = ContextAwareMessage.captureContextMetadata(new MyIncomingMetadata<>(message));
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }
}
