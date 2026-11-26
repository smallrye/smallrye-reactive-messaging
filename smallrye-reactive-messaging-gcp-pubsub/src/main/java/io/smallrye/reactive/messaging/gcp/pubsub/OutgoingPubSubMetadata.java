package io.smallrye.reactive.messaging.gcp.pubsub;

import java.util.HashMap;
import java.util.Map;

/**
 * Metadata of the messages sent to Pub/Sub: the attributes and the ordering key of the published message. Its data
 * comes from the message payload.
 * <p>
 * The outbound connector builds the published message from the payload and this metadata only. To propagate the
 * attributes and the ordering key of a message received from Pub/Sub, use {@link #from(IncomingPubSubMetadata)}.
 */
public class OutgoingPubSubMetadata {

    private final Map<String, String> attributes;
    private final String orderingKey;

    /**
     * @param attributes the attributes of the published message, can be {@code null}
     * @param orderingKey the ordering key of the published message, can be {@code null}
     */
    public OutgoingPubSubMetadata(Map<String, String> attributes, String orderingKey) {
        this.attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        this.orderingKey = orderingKey;
    }

    /**
     * @return the attributes, never {@code null}
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    /**
     * @return the ordering key, {@code null} if not set
     */
    public String getOrderingKey() {
        return orderingKey;
    }

    /**
     * @return a builder of metadata with no attributes and no ordering key
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder initialized with the attributes and the ordering key of a received message, to propagate them to
     * a message to send.
     *
     * @param incoming the metadata of the received message
     * @return the builder, to override or complete the copied attributes and ordering key
     */
    public static Builder from(IncomingPubSubMetadata incoming) {
        Builder builder = new Builder().withAttributes(incoming.getAttributes());
        if (!incoming.getOrderingKey().isEmpty()) {
            builder.withOrderingKey(incoming.getOrderingKey());
        }
        return builder;
    }

    /**
     * Builds {@link OutgoingPubSubMetadata}.
     */
    public static final class Builder {

        private final Map<String, String> attributes = new HashMap<>();
        private String orderingKey;

        private Builder() {
        }

        public Builder withAttribute(String key, String value) {
            attributes.put(key, value);
            return this;
        }

        public Builder withAttributes(Map<String, String> attributes) {
            if (attributes != null) {
                this.attributes.putAll(attributes);
            }
            return this;
        }

        public Builder withOrderingKey(String orderingKey) {
            this.orderingKey = orderingKey;
            return this;
        }

        public OutgoingPubSubMetadata build() {
            return new OutgoingPubSubMetadata(attributes, orderingKey);
        }
    }
}
