package io.smallrye.reactive.messaging.amqp;

public class AmqpHeaders {

    /**
     * The AMQP Address.
     * Type: String
     */
    public static final String ADDRESS = "amqp.address";

    /**
     * The Application Properties attached to the Message.
     * Type: {@link io.vertx.core.json.JsonObject}
     */
    public static final String APPLICATION_PROPERTIES = "amqp.application-properties";

    /**
     * The Content-Type of the message payload
     * Type: String
     */
    public static final String CONTENT_TYPE = "amqp.content-type";

    /**
     * The Content-Encoding of the message payload
     * Type: String
     */
    public static final String CONTENT_ENCODING = "amqp.content-encoding";

    /**
     * The correlation id of the message.
     * Type: String
     */
    public static final String CORRELATION_ID = "amqp.correlation-id";

    /**
     * The creation time of the message.
     * Type: long
     */
    public static final String CREATION_TIME = "amqp.creation-time";

    /**
     * The delivery count.
     * Type: int
     */
    public static final String DELIVERY_COUNT = "amqp.delivery-count";

    /**
     * The expiration time of the message.
     * Type: long
     */
    public static final String EXPIRY_TIME = "amqp.expiry-time";

    /**
     * The group Id of the message.
     * Type: String
     */
    public static final String GROUP_ID = "amqp.group-id";

    /**
     * The group sequence of the message.
     * Type: String
     */
    public static final String GROUP_SEQUENCE = "amqp.group-sequence";

    /**
     * The message id.
     * Type: String
     */
    public static final String ID = "amqp.id";

    /**
     * Whether the message is durable.
     * Type: boolean
     */
    public static final String DURABLE = "amqp.durable";

    /**
     * Whether this consumer is the first acquirer.
     * Type: boolean
     */
    public static final String FIRST_ACQUIRER = "amqp.first-acquirer";

    /**
     * The message priority
     * Type: int
     */
    public static final String PRIORITY = "amqp.priority";

    /**
     * The message subject
     * Type: String
     */
    public static final String SUBJECT = "amqp.subject";

    /**
     * The message time-to-live
     * Type: long
     */
    public static final String TTL = "amqp.ttl";

    /**
     * The message headers
     * Type: {@link org.apache.qpid.proton.amqp.messaging.Header}
     */
    public static final String HEADER = "amqp.headers";

    /**
     * Header to set the AMQP address for an outgoing message.
     * Type: String
     */
    public static final String OUTGOING_ADDRESS = "amqp.outgoing-address";

    /**
     * Header to set the AMQP application properties for an outgoing message.
     * Type: {@link io.vertx.core.json.JsonObject}
     */
    public static final String OUTGOING_APPLICATION_PROPERTIES = "amqp.outgoing-application-properties";

    /**
     * Header to set the content-type for an outgoing message.
     * Type: String
     */
    public static final String OUTGOING_CONTENT_TYPE = "amqp.outgoing-content-type";

    /**
     * Header to set the content-encoding for an outgoing message.
     * Type: String
     */
    public static final String OUTGOING_CONTENT_ENCODING = "amqp.outgoing-content-encoding";

    /**
     * Header to set the correlation-id for an outgoing message.
     * Type: String
     */
    public static final String OUTGOING_CORRELATION_ID = "amqp.outgoing-correlation-id";

    /**
     * Header to set the group-id for an outgoing message.
     * Type: String
     */
    public static final String OUTGOING_GROUP_ID = "amqp.outgoing-group-id";

    /**
     * Header to set the message id for an outgoing message.
     * Type: String
     */
    public static final String OUTGOING_ID = "amqp.outgoing-id";

    /**
     * Header to set whether an outgoing message is durable.
     * Type: boolean
     */
    public static final String OUTGOING_DURABLE = "amqp.outgoing-durable";

    /**
     * Header to set the priority of an outgoing message.
     * Type: int
     */
    public static final String OUTGOING_PRIORITY = "amqp.outgoing-priority";

    /**
     * Header to set the subject of an outgoing message.
     * Type: String
     */
    public static final String OUTGOING_SUBJECT = "amqp.outgoing-subject";

    /**
     * Header to set the ttl of an outgoing message.
     * Type: long
     */
    public static final String OUTGOING_TTL = "amqp.outgoing-ttl";

    private AmqpHeaders() {
        // avoid direct instantiation.
    }
}
