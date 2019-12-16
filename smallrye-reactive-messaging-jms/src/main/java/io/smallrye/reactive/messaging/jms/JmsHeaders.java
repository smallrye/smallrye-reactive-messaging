package io.smallrye.reactive.messaging.jms;

public class JmsHeaders {

    /**
     * Gets the message id of the incoming JMS message.
     * Type: String
     */
    public static final String MESSAGE_ID = "jms.message-id";

    /**
     * Gets the message timestamp, i.e. the time a message was handed off to a JMS provider to be sent.
     * Type: long
     */
    public static final String MESSAGE_TIMESTAMP = "jms.timestamp";

    /**
     * Gets the correlation ID of the incoming JMS message.
     * Type: String
     */
    public static final String CORRELATION_ID = "jms.correlation-id";

    /**
     * Gets the {@link javax.jms.Destination} to which a reply to the incoming JMS message should be sent.
     * Type: {@link javax.jms.Destination}
     */
    public static final String REPLY_TO = "jms.reply-to";

    /**
     * Gets the {@link javax.jms.Destination} to which the incoming JMS message has been sent.
     * Type: {@link javax.jms.Destination}
     */
    public static final String DESTINATION = "jms.destination";

    /**
     * Gets the delivery mode for the incoming JMS message.
     * Type: int
     */
    public static final String DELIVERY_MODE = "jms.delivery-mode";

    /**
     * Gets whether the incoming JMS message is being redelivered.
     * Type: boolean
     */
    public static final String REDELIVERED = "jms.redelivered";

    /**
     * Gets the message type identifier for the incoming JMS message.
     * Type: String
     */
    public static final String TYPE = "jms.type";

    /**
     * Gets the message expiration time for the incoming JMS message.
     * Type: long
     */
    public static final String EXPIRATION = "jms.expiration";

    /**
     * Gets the message delivery time.
     * Type: long
     */
    public static final String DELIVERY_TIME = "jms.delivery-time";

    /**
     * Gets the priority of the incoming JMS message.
     * 0-4 indicates normal priorities.
     * 5-9 indicates expedited priorities.
     * Type: int
     */
    public static final String PRIORITY = "jms.priority";

    /**
     * Gets the properties of the incoming JMS message.
     * Type: {@link JmsProperties}
     */
    public static final String PROPERTIES = "jms.properties";

    /**
     * Gets the correlation ID of the outgoing JMS message.
     * Type: String
     */
    public static final String OUTGOING_CORRELATION_ID = "jms.outgoing-correlation-id";

    /**
     * Gets the {@link javax.jms.Destination} to which a reply to the outgoing JMS message should be sent.
     * Type: {@link javax.jms.Destination}
     */
    public static final String OUTGOING_REPLY_TO = "jms.outgoing-reply-to";

    /**
     * Gets the {@link javax.jms.Destination} to which the outgoing JMS message should be sent.
     * Type: {@link javax.jms.Destination}
     */
    public static final String OUTGOING_DESTINATION = "jms.outgoing-destination";

    /**
     * Gets the delivery mode for the outgoing JMS message.
     * Type: int
     * 
     * @see javax.jms.DeliveryMode
     */
    public static final String OUTGOING_DELIVERY_MODE = "jms.outgoing-delivery-mode";

    /**
     * Gets the message type identifier for the outgoing JMS message.
     * Type: String
     */
    public static final String OUTGOING_TYPE = "jms.outgoing-type";

    /**
     * Gets the properties of the incoming JMS message.
     * Type: {@link JmsProperties}
     * Use {@link JmsPropertiesBuilder} using {@link JmsProperties#builder()} to create an instance of
     * {@link JmsProperties}.
     */
    public static final String OUTGOING_PROPERTIES = "jms.outgoing-properties";

    private JmsHeaders() {
        // avoid direct instantiation
    }
}
