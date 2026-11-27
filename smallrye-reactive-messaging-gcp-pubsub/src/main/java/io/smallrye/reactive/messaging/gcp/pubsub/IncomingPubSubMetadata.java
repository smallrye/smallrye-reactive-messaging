package io.smallrye.reactive.messaging.gcp.pubsub;

import java.util.Map;
import java.util.Objects;

import com.google.pubsub.v1.PubsubMessage;

/**
 * Metadata of the messages received from Pub/Sub: the received message, its attributes and its ordering key.
 * <p>
 * The messages sent are not given the attributes and ordering key of the message they come from: use
 * {@link OutgoingPubSubMetadata#from(IncomingPubSubMetadata)} to propagate them.
 */
public class IncomingPubSubMetadata {

    private final PubsubMessage message;

    public IncomingPubSubMetadata(PubsubMessage message) {
        this.message = Objects.requireNonNull(message);
    }

    /**
     * @return the received message
     */
    public PubsubMessage getMessage() {
        return message;
    }

    /**
     * @return the attributes of the received message, never {@code null}
     */
    public Map<String, String> getAttributes() {
        return message.getAttributesMap();
    }

    /**
     * @return the ordering key of the received message, empty if not set
     */
    public String getOrderingKey() {
        return message.getOrderingKey();
    }
}
