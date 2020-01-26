package io.smallrye.reactive.messaging.amqp;

import io.vertx.core.json.JsonObject;

public class OutgoingAmqpMetadata {
    /**
     * The AMQP address for an outgoing message.
     */
    private final String address;

    /**
     * The AMQP application properties for an outgoing message.
     */
    private final JsonObject properties;

    /**
     * The content-type for an outgoing message.
     */
    private final String contentType;

    /**
     * The content-encoding for an outgoing message.
     */
    private final String contentEncoding;

    /**
     * The correlation-id for an outgoing message.
     */
    private final String correlationId;

    /**
     * The group-id for an outgoing message.
     */
    private final String groupId;

    /**
     * The message id for an outgoing message.
     */
    private final String id;

    /**
     * Whether an outgoing message is durable.
     */
    private final boolean durable;

    /**
     * The priority of an outgoing message.
     * Type: int
     */
    private final int priority;

    /**
     * The subject of an outgoing message.
     */
    private final String subject;

    /**
     * The ttl of an outgoing message.
     */
    private final long ttl;

    public OutgoingAmqpMetadata(String address, JsonObject properties, String contentType,
            String contentEncoding, String correlationId, String groupId, String id, boolean durable, int priority,
            String subject, long ttl) {
        this.address = address;
        this.properties = properties;
        this.contentType = contentType;
        this.contentEncoding = contentEncoding;
        this.correlationId = correlationId;
        this.groupId = groupId;
        this.id = id;
        this.durable = durable;
        this.priority = priority;
        this.subject = subject;
        this.ttl = ttl;
    }

    public String getAddress() {
        return address;
    }

    public JsonObject getProperties() {
        return properties;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getId() {
        return id;
    }

    public boolean isDurable() {
        return durable;
    }

    public int getPriority() {
        return priority;
    }

    public String getSubject() {
        return subject;
    }

    public long getTtl() {
        return ttl;
    }

    public static OutgoingAmqpMetadataBuilder builder() {
        return new OutgoingAmqpMetadataBuilder();
    }

    public static final class OutgoingAmqpMetadataBuilder {
        private String address;
        private JsonObject properties = new JsonObject();
        private String contentType;
        private String contentEncoding;
        private String correlationId;
        private String groupId;
        private String id;
        private boolean durable;
        private int priority;
        private String subject;
        private long ttl;

        private OutgoingAmqpMetadataBuilder() {
        }

        public static OutgoingAmqpMetadataBuilder anOutgoingAmqpMetadata() {
            return new OutgoingAmqpMetadataBuilder();
        }

        public OutgoingAmqpMetadataBuilder withAddress(String address) {
            this.address = address;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withProperties(JsonObject properties) {
            this.properties = properties;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withContentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withContentEncoding(String contentEncoding) {
            this.contentEncoding = contentEncoding;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withCorrelationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withDurable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withSubject(String subject) {
            this.subject = subject;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withTtl(long ttl) {
            this.ttl = ttl;
            return this;
        }

        public OutgoingAmqpMetadata build() {
            return new OutgoingAmqpMetadata(address, properties, contentType, contentEncoding, correlationId, groupId,
                    id, durable, priority, subject, ttl);
        }
    }
}
