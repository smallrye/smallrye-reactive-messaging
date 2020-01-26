package io.smallrye.reactive.messaging.jms;

import javax.jms.Destination;

public interface JmsMessageMetadata {

    String getCorrelationId();

    Destination getReplyTo();

    Destination getDestination();

    int getDeliveryMode();

    String getType();

    JmsProperties getProperties();
}
