package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarMessages.msg;

import java.util.Objects;

import org.apache.pulsar.client.api.Messages;

public class PulsarIncomingBatchMessageMetadata {
    private final Messages<?> delegate;

    public PulsarIncomingBatchMessageMetadata(Messages<?> messages) {
        this.delegate = Objects.requireNonNull(messages, msg.isRequired("messages"));
    }

    public <T> Messages<T> getMessages() {
        return (Messages<T>) delegate;
    }

}
