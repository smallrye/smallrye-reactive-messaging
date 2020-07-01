package io.smallrye.reactive.messaging.amqp;

import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;

import io.vertx.amqp.AmqpMessage;
import io.vertx.core.json.JsonObject;

public class IncomingAmqpMetadata {

    private final AmqpMessage message;

    public IncomingAmqpMetadata(AmqpMessage message) {
        this.message = message;
    }

    /**
     * The AMQP address of the message.
     * <p>
     * The address is stored in the {@code to} field which identifies the node that is the intended destination of the
     * message. On any given transfer this might not be the node at the receiving end of the link.
     * <p>
     * Stored in the message properties.
     *
     * @return the address
     */
    public String getAddress() {
        return message.address();
    }

    /**
     * The application-properties section is a part of the bare message used for structured application data.
     * Routers and brokers can use the data within this structure for the purposes of filtering or routing.
     * <p>
     * The keys of this map are restricted to be of type string and the values are restricted to be of simple types only,
     * that is, excluding map, list, and array types.
     *
     * @return the application properties
     * @apiNote http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-application-properties
     */
    public JsonObject getProperties() {
        return message.applicationProperties();
    }

    /**
     * The RFC-2046 MIME type for the message's application-data section (body).
     * As per RFC-2046 this can contain a charset parameter defining the character encoding used: e.g.,
     * 'text/plain; charset="utf-8"'.
     * <p>
     * If the payload is known to be truly opaque binary data, the content-type should be set to application/octet-stream.
     * <p>
     * Stored in the message properties.
     */
    public String getContentType() {
        return message.unwrap().getContentType();
    }

    /**
     * The content-encoding property is used as a modifier to the content-type.
     * When present, its value indicates what additional content encodings have been applied to the application-data,
     * and thus what decoding mechanisms need to be applied in order to obtain the media-type referenced by the
     * content-type header field.
     * <p>
     * Stored in the message properties.
     *
     * @return the content-encoding
     */
    public String getContentEncoding() {
        return message.unwrap().getContentEncoding();
    }

    /**
     * An absolute time when this message was created.
     * <p>
     * Stored in the message properties.
     */
    public long getCreationTime() {
        return message.creationTime();
    }

    /**
     * The number of unsuccessful previous attempts to deliver this message. If this value is non-zero it can be taken
     * as an indication that the delivery might be a duplicate. On first delivery, the value is zero. It is incremented
     * upon an outcome being settled at the sender, according to rules defined for each outcome.
     * <p>
     * Stored in the message header
     *
     * @return the delivery count
     */
    public int getDeliveryCount() {
        return message.deliveryCount();
    }

    /**
     * An absolute time when this message is considered to be expired.
     * <p>
     * Stored in the message properties.
     *
     * @return the expiry time, 0 is none
     */
    public long getExpiryTime() {
        return message.expiryTime();
    }

    /**
     * Identifies the group the message belongs to.
     * <p>
     * Stored in the message properties.
     *
     * @return the group id if set
     */
    public String getGroupId() {
        return message.groupId();
    }

    /**
     * The relative position of this message within its group.
     * <p>
     * Stored in the message properties.
     *
     * @return the sequence number, 0 if not set
     */
    public long getGroupSequence() {
        return message.groupSequence();
    }

    /**
     * The message-id, if set, uniquely identifies a message within the message system.
     * The message producer is usually responsible for setting the message-id in such a way that it is assured to be
     * globally unique. A broker may discard a message as a duplicate if the value of the message-id matches that of a
     * previously received message sent to the same node.
     * <p>
     * Stored in the message properties.
     *
     * @return the id
     */
    public String getId() {
        return message.id();
    }

    /**
     * The identity of the user responsible for producing the message.
     * The client sets this value, and it may be authenticated by intermediaries.
     * <p>
     * Stored in the message properties.
     *
     * @return the user id
     */
    public String getUserId() {
        byte[] userId = message.unwrap().getUserId();
        if (userId != null) {
            return new String(userId);
        } else {
            return null;
        }
    }

    /**
     * Whether the message is durable.
     * <p>
     * Durable messages must not be lost even if an intermediary is unexpectedly terminated and restarted. A target
     * which is not capable of fulfilling this guarantee must not accept messages where the durable header is set to
     * true: if the source allows the rejected outcome then the message should be rejected with the precondition-failed
     * error, otherwise the link must be detached by the receiver with the same error.
     * <p>
     * Stored in the message header.
     *
     * @return whether the message is marked as durable
     */
    public boolean isDurable() {
        return message.isDurable();
    }

    /**
     * If this value is true, then this message has not been acquired by any other link. If this value is false, then
     * this message may have previously been acquired by another link or links.
     * <p>
     * Stored in the message header.
     *
     * @return whether this message has been acquired by another link before.
     */
    public boolean isFirstAcquirer() {
        return message.isFirstAcquirer();
    }

    /**
     * This priority field contains the relative message priority. Higher numbers indicate higher priority messages.
     * Messages with higher priorities may be delivered before those with lower priorities.
     * <p>
     * Stored in the message header.
     *
     * @return the priority
     */
    public short getPriority() {
        return (short) message.priority();
    }

    /**
     * A common field for summary information about the message content and purpose.
     * <p>
     * Stored in the message properties.
     *
     * @return the subject if any
     */
    public String getSubject() {
        return message.subject();
    }

    /**
     * The address of the node to send replies to.
     * <p>
     * Stored in the message properties.
     *
     * @return the reply-to address
     */
    public String getReplyTo() {
        return message.replyTo();
    }

    /**
     * This is a client-specific id that is used so that client can send replies to this message to a specific group.
     * <p>
     * Stored in the message properties.
     *
     * @return the reply-to address
     */
    public String getReplyToGroupId() {
        return message.unwrap().getReplyToGroupId();
    }

    /**
     * This is a client-specific id that can be used to mark or identify messages between clients.
     * <p>
     * Stored in the message properties.
     *
     * @return the reply-to address
     */
    public String getCorrelationId() {
        return message.correlationId();
    }

    /**
     * Duration in milliseconds for which the message is to be considered "live". If this is set then a message
     * expiration time will be computed based on the time of arrival at an intermediary. Messages that live longer
     * than their expiration time will be discarded (or dead lettered). When a message is transmitted by an
     * intermediary that was received with a ttl, the transmitted message's header SHOULD contain a ttl that is
     * computed as the difference between the current time and the formerly computed message expiration time, i.e., t
     * he reduced ttl, so that messages will eventually die if they end up in a delivery loop.
     * <p>
     * Stored in the message header
     *
     * @return the ttl, 0 if not set
     */
    public long getTtl() {
        return message.ttl();
    }

    /**
     * @return the expiry time
     * @deprecated Use {@link #getExpiryTime()} instead
     */
    @Deprecated
    public long getExpirationTime() {
        return getExpiryTime();
    }

    /**
     * The delivery annotations.
     * <p>
     * The delivery-annotations section is used for delivery-specific non-standard properties at the head of the message.
     * Delivery annotations convey information from the sending peer to the receiving peer. If the recipient does not
     * understand the annotation it cannot be acted upon and its effects (such as any implied propagation) cannot be
     * acted upon. Annotations might be specific to one implementation, or common to multiple implementations.
     * The capabilities negotiated on link attach and on the source and target should be used to establish which
     * annotations a peer supports. A registry of defined annotations and their meanings is maintained.
     * <p>
     * The symbolic key "rejected" is reserved for the use of communicating error information regarding rejected
     * messages. Any values associated with the "rejected" key must be of type error.
     * <p>
     * If the delivery-annotations section is omitted, it is equivalent to a delivery-annotations section containing an
     * empty map of annotations.
     *
     * @apiNote http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-delivery-annotations
     */
    public DeliveryAnnotations getDeliveryAnnotations() {
        return message.unwrap().getDeliveryAnnotations();
    }

    /**
     * The message annotations.
     * <p>
     * The message-annotations section is used for properties of the message which are aimed at the infrastructure and
     * should be propagated across every delivery step. Message annotations convey information about the message.
     * Intermediaries must propagate the annotations unless the annotations are explicitly augmented or modified
     * (e.g., by the use of the modified outcome).
     * <p>
     * The capabilities negotiated on link attach and on the source and target can be used to establish which
     * annotations a peer understands; however, in a network of AMQP intermediaries it might not be possible to know if
     * every intermediary will understand the annotation. Note that for some annotations it might not be necessary for
     * the intermediary to understand their purpose, i.e., they could be used purely as an attribute which can be
     * filtered on.
     * <p>
     * A registry of defined annotations and their meanings is maintained.
     * <p>
     * If the message-annotations section is omitted, it is equivalent to a message-annotations section containing an
     * empty map of annotations.
     *
     * @apiNote http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-message-annotations
     */
    public MessageAnnotations getMessageAnnotations() {
        return message.unwrap().getMessageAnnotations();
    }

    /**
     * Transport footers for a message.
     * The footer section is used for details about the message or delivery which can only be calculated or evaluated
     * once the whole bare message has been constructed or seen (for example message hashes, HMACs, signatures and
     * encryption details).
     */
    public Footer getFooter() {
        return message.unwrap().getFooter();
    }
}
