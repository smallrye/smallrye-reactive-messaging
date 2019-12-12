package io.smallrye.reactive.messaging.amqp;

public class AmqpHeaders {

    public static final String ADDRESS = "jms.address";
    public static final String APPLICATION_PROPERTIES = "jms.application-properties";
    public static final String CONTENT_TYPE = "jms.contentType";
    public static final String CONTENT_ENCODING = "jms.contentEncoding";
    public static final String CORRELATION_ID = "jms.correlationId";
    public static final String CREATION_TIME = "jms.creationTime";
    public static final String DELIVERY_COUNT = "jms.deliveryCount";
    public static final String EXPIRY_TIME = "jms.expiryTime";
    public static final String GROUP_ID = "jms.groupId";
    public static final String GROUP_SEQUENCE = "jms.groupSequence";
    public static final String ID = "jms.id";
    public static final String DURABLE = "jms.durable";
    public static final String FIRST_ACQUIRER = "jms.first-acquirer";
    public static final String PRIORITY = "jms.priority";
    public static final String SUBJECT = "jms.subject";
    public static final String TTL = "jms.ttl";
    public static final String HEADER = "jms.headers";

    private AmqpHeaders() {
        // avoid direct instantiation.
    }
}
