package io.smallrye.reactive.messaging.amqp;

import org.apache.qpid.proton.amqp.messaging.Header;

import io.vertx.core.json.JsonObject;

public class IncomingAmqpMetadata {

    /**
     * The AMQP Address.
     */
    private final String address;

    /**
     * The Application Properties attached to the Message.
     */
    private final JsonObject properties;

    /**
     * The Content-Type of the message payload
     */
    private final String contentType;

    /**
     * The Content-Encoding of the message payload
     */
    private final String contentEncoding;

    /**
     * The correlation id of the message.
     */
    private final String correlationId;

    /**
     * The creation time of the message.
     */
    private final long creationTime;

    /**
     * The delivery count.
     */
    private final int deliveryCount;

    /**
     * The expiration time of the message.
     */
    private final long expirationTime;

    /**
     * The group Id of the message.
     */
    private final String groupId;

    /**
     * The group sequence of the message.
     */
    private final long groupSequence;

    /**
     * The message id.
     */
    private final String id;

    /**
     * Whether the message is durable.
     */
    private final boolean durable;

    /**
     * Whether this consumer is the first acquirer.
     */
    private final boolean firstAcquirer;

    /**
     * The message priority
     */
    private final int priority;

    /**
     * The message subject
     */
    private final String subject;

    /**
     * The message time-to-live
     */
    private final long ttl;

    /**
     * The message header
     */
    private final Header header;

    public IncomingAmqpMetadata(String address, JsonObject properties, String contentType,
            String contentEncoding, String correlationId, long creationTime, int deliveryCount, long expirationTime,
            String groupId, long groupSequence, String id, boolean durable, boolean firstAcquirer, int priority,
            String subject, long ttl, Header header) {
        this.address = address;
        this.properties = properties;
        this.contentType = contentType;
        this.contentEncoding = contentEncoding;
        this.correlationId = correlationId;
        this.creationTime = creationTime;
        this.deliveryCount = deliveryCount;
        this.expirationTime = expirationTime;
        this.groupId = groupId;
        this.groupSequence = groupSequence;
        this.id = id;
        this.durable = durable;
        this.firstAcquirer = firstAcquirer;
        this.priority = priority;
        this.subject = subject;
        this.ttl = ttl;
        this.header = header;
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

    public long getCreationTime() {
        return creationTime;
    }

    public int getDeliveryCount() {
        return deliveryCount;
    }

    public long getExpiryTime() {
        return expirationTime;
    }

    public String getGroupId() {
        return groupId;
    }

    public long getGroupSequence() {
        return groupSequence;
    }

    public String getId() {
        return id;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isFirstAcquirer() {
        return firstAcquirer;
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

    public Header getHeader() {
        return header;
    }

    static final class IncomingAmqpMetadataBuilder {
        private JsonObject properties = new JsonObject();
        private String address;
        private String contentType;
        private String contentEncoding;
        private String correlationId;
        private long creationTime;
        private int deliveryCount;
        private long expirationTime;
        private String groupId;
        private long groupSequence;
        private String id;
        private boolean durable;
        private boolean firstAcquirer;
        private int priority = -1;
        private String subject;
        private long ttl;
        private Header header = new Header();

        public IncomingAmqpMetadataBuilder withAddress(String address) {
            this.address = address;
            return this;
        }

        public IncomingAmqpMetadataBuilder withProperties(JsonObject properties) {
            this.properties = properties;
            return this;
        }

        public IncomingAmqpMetadataBuilder withContentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public IncomingAmqpMetadataBuilder withContentEncoding(String contentEncoding) {
            this.contentEncoding = contentEncoding;
            return this;
        }

        public IncomingAmqpMetadataBuilder withCorrelationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public IncomingAmqpMetadataBuilder withCreationTime(long creationTime) {
            this.creationTime = creationTime;
            return this;
        }

        public IncomingAmqpMetadataBuilder withDeliveryCount(int deliveryCount) {
            this.deliveryCount = deliveryCount;
            return this;
        }

        public IncomingAmqpMetadataBuilder withExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
            return this;
        }

        public IncomingAmqpMetadataBuilder withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public IncomingAmqpMetadataBuilder withGroupSequence(long groupSequence) {
            this.groupSequence = groupSequence;
            return this;
        }

        public IncomingAmqpMetadataBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public IncomingAmqpMetadataBuilder withDurable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public IncomingAmqpMetadataBuilder withFirstAcquirer(boolean firstAcquirer) {
            this.firstAcquirer = firstAcquirer;
            return this;
        }

        public IncomingAmqpMetadataBuilder withPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public IncomingAmqpMetadataBuilder withSubject(String subject) {
            this.subject = subject;
            return this;
        }

        public IncomingAmqpMetadataBuilder withTtl(long ttl) {
            this.ttl = ttl;
            return this;
        }

        public IncomingAmqpMetadataBuilder withHeader(Header header) {
            this.header = header;
            return this;
        }

        public IncomingAmqpMetadata build() {
            return new IncomingAmqpMetadata(address, properties, contentType, contentEncoding, correlationId,
                    creationTime, deliveryCount, expirationTime, groupId, groupSequence, id, durable, firstAcquirer,
                    priority, subject, ttl, header);
        }
    }
}
