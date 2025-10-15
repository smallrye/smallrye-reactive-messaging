package io.smallrye.reactive.messaging.jms;

import jakarta.jms.Destination;

public class OutgoingJmsMessageMetadata implements JmsMessageMetadata {

    private final String correlationId;
    private final Destination replyTo;
    private final Destination destination;
    private final int deliveryMode;
    private final String type;
    private final Integer priority;
    private final JmsProperties properties;

    public OutgoingJmsMessageMetadata(String correlationId, Destination replyTo, Destination destination,
            int deliveryMode, String type, JmsProperties properties) {
        this.correlationId = correlationId;
        this.replyTo = replyTo;
        this.destination = destination;
        this.deliveryMode = deliveryMode;
        this.type = type;
        this.priority = null;
        this.properties = properties;
    }

    public OutgoingJmsMessageMetadata(final String correlationId, final Destination replyTo, final Destination destination,
            final int deliveryMode, final String type, final Integer priority, final JmsProperties properties) {
        this.correlationId = correlationId;
        this.replyTo = replyTo;
        this.destination = destination;
        this.deliveryMode = deliveryMode;
        this.type = type;
        this.priority = priority;
        this.properties = properties;
    }

    @Override
    public String getCorrelationId() {
        return correlationId;
    }

    @Override
    public Destination getReplyTo() {
        return replyTo;
    }

    @Override
    public Destination getDestination() {
        return destination;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public String getType() {
        return type;
    }

    public Integer getPriority() {
        return priority;
    }

    @Override
    public JmsProperties getProperties() {
        return properties;
    }

    public static OutgoingJmsMessageMetadataBuilder builder() {
        return new OutgoingJmsMessageMetadataBuilder();
    }

    public static final class OutgoingJmsMessageMetadataBuilder {
        private String correlationId;
        private Destination replyTo;
        private Destination destination;
        private int deliveryMode = -1;
        private String type;
        private Integer priority;
        private JmsProperties properties;

        public OutgoingJmsMessageMetadataBuilder withCorrelationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public OutgoingJmsMessageMetadataBuilder withReplyTo(Destination replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        public OutgoingJmsMessageMetadataBuilder withDestination(Destination destination) {
            this.destination = destination;
            return this;
        }

        public OutgoingJmsMessageMetadataBuilder withDeliveryMode(int deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        public OutgoingJmsMessageMetadataBuilder withType(String type) {
            this.type = type;
            return this;
        }

        public OutgoingJmsMessageMetadataBuilder withPriority(final Integer priority) {
            this.priority = priority;
            return this;
        }

        public OutgoingJmsMessageMetadataBuilder withProperties(JmsProperties properties) {
            this.properties = properties;
            return this;
        }

        public OutgoingJmsMessageMetadata build() {
            return new OutgoingJmsMessageMetadata(correlationId, replyTo, destination, deliveryMode, type, priority,
                    properties);
        }
    }
}
