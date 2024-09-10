package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarMessages.msg;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.Messages;
import org.eclipse.microprofile.reactive.messaging.Message;

public class PulsarIncomingBatchMessageMetadata {
    private final Messages<?> delegate;
    private final List<PulsarIncomingMessage<?>> batchedMessages;

    public PulsarIncomingBatchMessageMetadata(Messages<?> messages) {
        this(messages, Collections.emptyList());
    }

    public <T> PulsarIncomingBatchMessageMetadata(Messages<T> messages, List<PulsarIncomingMessage<?>> batchedMessages) {
        this.delegate = Objects.requireNonNull(messages, msg.isRequired("messages"));
        this.batchedMessages = batchedMessages;
    }

    public <T> Messages<T> getMessages() {
        return (Messages<T>) delegate;
    }

    public List<PulsarIncomingMessage<?>> getIncomingMessages() {
        return batchedMessages;
    }

    /**
     * Get metadata object for the given Pulsar Message.
     * This method is useful when you need to access metadata for a specific message in the batch.
     *
     * @param msg Pulsar message
     * @param metadata metadata type class
     * @param <M> metadata type
     * @return the metadata object for the given message
     */
    public <M> M getMetadataForMessage(org.apache.pulsar.client.api.Message<?> msg, Class<M> metadata) {
        for (Message<?> record : batchedMessages) {
            if (record.getMetadata().get(PulsarIncomingMessageMetadata.class).orElseThrow().getMessage().equals(msg)) {
                return record.getMetadata(metadata).orElse(null);
            }
        }
        return null;
    }

}
