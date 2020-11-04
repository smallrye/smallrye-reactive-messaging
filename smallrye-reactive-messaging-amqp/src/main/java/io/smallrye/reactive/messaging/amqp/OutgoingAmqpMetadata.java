package io.smallrye.reactive.messaging.amqp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;

import io.vertx.core.json.JsonObject;

public class OutgoingAmqpMetadata {

    private final Message message;

    public OutgoingAmqpMetadata() {
        message = Message.Factory.create();
    }

    public OutgoingAmqpMetadata(String address, boolean durable, short priority, long ttl,
            DeliveryAnnotations deliveryAnnotations, MessageAnnotations messageAnnotations, String id, String userId,
            String subject, String replyTo, String correlationId, String contentType, String contentEncoding,
            long expiryTime, long creationTime, String groupId, long groupSequence, String replyToGroupId,
            JsonObject applicationProperties, Footer footer) {
        this();
        message.setAddress(address);
        message.setDurable(durable);
        if (priority > 0) {
            message.setPriority(priority);
        }
        if (ttl > 0) {
            message.setTtl(ttl);
        }

        message.setDeliveryAnnotations(deliveryAnnotations);
        message.setMessageAnnotations(messageAnnotations);

        message.setMessageId(id);
        if (userId != null) {
            message.setUserId(userId.getBytes());
        }
        message.setSubject(subject);
        message.setReplyTo(replyTo);
        message.setCorrelationId(correlationId);
        message.setContentType(contentType);
        message.setContentEncoding(contentEncoding);
        if (expiryTime > 0) {
            message.setExpiryTime(expiryTime);
        }
        if (creationTime > 0) {
            message.setCreationTime(creationTime);
        }
        message.setGroupId(groupId);
        if (groupSequence >= 0) {
            message.setGroupSequence(groupSequence);
        }
        message.setReplyToGroupId(replyToGroupId);

        message.setApplicationProperties(new ApplicationProperties(applicationProperties.getMap()));

        message.setFooter(footer);
    }

    public String getAddress() {
        return message.getAddress();
    }

    public String getUserId() {
        byte[] userId = message.getUserId();
        if (userId != null) {
            return new String(userId);
        }
        return null;
    }

    public String getReplyTo() {
        return message.getReplyTo();
    }

    public String getGroupId() {
        return message.getGroupId();
    }

    public String getContentType() {
        return message.getContentType();
    }

    public long getExpiryTime() {
        return message.getExpiryTime();
    }

    public String getCorrelationId() {
        Object correlationId = message.getCorrelationId();
        if (correlationId != null) {
            return correlationId.toString();
        }
        return null;
    }

    public String getContentEncoding() {
        return message.getContentEncoding();
    }

    public String getSubject() {
        return message.getSubject();
    }

    public DeliveryAnnotations getDeliveryAnnotations() {
        DeliveryAnnotations annotations = message.getDeliveryAnnotations();
        if (annotations == null) {
            return new DeliveryAnnotations(Collections.emptyMap());
        }
        return annotations;
    }

    public MessageAnnotations getMessageAnnotations() {
        MessageAnnotations annotations = message.getMessageAnnotations();
        if (annotations == null) {
            return new MessageAnnotations(Collections.emptyMap());
        }
        return annotations;
    }

    public Footer getFooter() {
        Footer footer = message.getFooter();
        if (footer == null) {
            return new Footer(Collections.emptyMap());
        }
        return footer;
    }

    public boolean isDurable() {
        return message.isDurable();
    }

    public short getPriority() {
        return message.getPriority();
    }

    public long getTtl() {
        return message.getTtl();
    }

    public String getMessageId() {
        Object messageId = message.getMessageId();
        if (messageId != null) {
            return messageId.toString();
        }
        return null;
    }

    public long getGroupSequence() {
        return message.getGroupSequence();
    }

    public String getReplyToGroupId() {
        return message.getReplyToGroupId();
    }

    public long getCreationTime() {
        return message.getCreationTime();
    }

    public JsonObject getProperties() {
        ApplicationProperties applicationProperties = message.getApplicationProperties();
        if (applicationProperties != null) {
            return new JsonObject(applicationProperties.getValue());
        } else {
            return new JsonObject();
        }
    }

    public static OutgoingAmqpMetadataBuilder builder() {
        return new OutgoingAmqpMetadataBuilder();
    }

    public static OutgoingAmqpMetadataBuilder from(OutgoingAmqpMetadata existing) {
        return new OutgoingAmqpMetadataBuilder(existing);
    }

    public static final class OutgoingAmqpMetadataBuilder {

        // Header
        private boolean durable;
        private short priority = -1;
        private long ttl = -1;

        private Map<Symbol, Object> deliveryAnnotations = new HashMap<>();
        private Map<Symbol, Object> messageAnnotations = new HashMap<>();

        // Properties
        private String address; // to field
        private String id;
        private String userId;
        private String subject;
        private String replyTo;
        private String correlationId;
        private String contentType;
        private String contentEncoding;
        private long expiryTime = -1;
        private long creationTime = -1;
        private String groupId;
        private long groupSequence = -1;
        private String replyToGroupId;

        private JsonObject applicationProperties = new JsonObject();

        private final Map<String, Object> footer = new HashMap<>();

        private OutgoingAmqpMetadataBuilder() {
        }

        private OutgoingAmqpMetadataBuilder(OutgoingAmqpMetadata existing) {
            this.address = existing.message.getAddress();

            this.durable = existing.isDurable();
            this.priority = existing.getPriority();
            this.ttl = existing.getTtl();

            this.applicationProperties = existing.getProperties();

            this.deliveryAnnotations.putAll(existing.getDeliveryAnnotations().getValue());
            this.messageAnnotations.putAll(existing.getMessageAnnotations().getValue());

            this.id = existing.getMessageId();
            this.userId = existing.getUserId();
            this.subject = existing.getSubject();
            this.replyTo = existing.getReplyTo();
            this.correlationId = existing.getCorrelationId();
            this.contentType = existing.getContentType();
            this.contentEncoding = existing.getContentEncoding();
            this.expiryTime = existing.getExpiryTime();
            this.creationTime = existing.getCreationTime();
            this.groupId = existing.getGroupId();
            this.groupSequence = existing.getGroupSequence();
            this.replyToGroupId = existing.getReplyToGroupId();

            @SuppressWarnings("unchecked")
            Map<String, Object> footer = existing.getFooter().getValue();
            this.footer.putAll(footer);
        }

        public OutgoingAmqpMetadataBuilder withAddress(String address) {
            this.address = address;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withDurable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withPriority(short priority) {
            this.priority = priority;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withTtl(long ttl) {
            this.ttl = ttl;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withDeliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
            this.deliveryAnnotations = deliveryAnnotations.getValue();
            return this;
        }

        public OutgoingAmqpMetadataBuilder withMessageAnnotations(MessageAnnotations messageAnnotations) {
            this.messageAnnotations = messageAnnotations.getValue();
            return this;
        }

        public OutgoingAmqpMetadataBuilder withDeliveryAnnotations(String key, Object value) {
            if (this.deliveryAnnotations == null) {
                this.deliveryAnnotations = new HashMap<>();
            }
            this.deliveryAnnotations.put(Symbol.valueOf(key), value);
            return this;
        }

        public OutgoingAmqpMetadataBuilder withMessageAnnotations(String key, Object value) {
            if (this.messageAnnotations == null) {
                this.messageAnnotations = new HashMap<>();
            }
            this.messageAnnotations.put(Symbol.valueOf(key), value);
            return this;
        }

        public OutgoingAmqpMetadataBuilder withMessageId(String id) {
            this.id = id;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withReplyTo(String replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withCorrelationId(String correlationId) {
            this.correlationId = correlationId;
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

        public OutgoingAmqpMetadataBuilder withExpiryTime(long expiryTime) {
            this.expiryTime = expiryTime;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withCreationTime(long creationTime) {
            this.creationTime = creationTime;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withGroupSequence(int groupSequence) {
            this.groupSequence = groupSequence;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withReplyToGroupId(String replyToGroupId) {
            this.replyToGroupId = replyToGroupId;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withSubject(String subject) {
            this.subject = subject;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withApplicationProperty(String key, Object value) {
            applicationProperties.put(key, value);
            return this;
        }

        public OutgoingAmqpMetadataBuilder withApplicationProperties(JsonObject json) {
            applicationProperties = json;
            return this;
        }

        public OutgoingAmqpMetadataBuilder withFooter(String key, Object value) {
            footer.put(key, value);
            return this;
        }

        public OutgoingAmqpMetadata build() {
            return new OutgoingAmqpMetadata(
                    address,
                    // header
                    durable, priority, ttl,
                    new DeliveryAnnotations(deliveryAnnotations),
                    new MessageAnnotations(messageAnnotations),
                    // properties
                    id, userId, subject, replyTo, correlationId, contentType, contentEncoding, expiryTime, creationTime,
                    groupId, groupSequence, replyToGroupId,

                    // application properties,
                    applicationProperties,

                    new Footer(footer));
        }
    }

}
