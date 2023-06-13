package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.Consumer;

import io.smallrye.mutiny.Uni;

public interface PulsarAckHandler {

    interface Factory {
        PulsarAckHandler create(Consumer<?> consumer, PulsarConnectorIncomingConfiguration config);
    }

    Uni<Void> handle(PulsarIncomingMessage<?> message);

}
