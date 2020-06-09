package io.smallrye.reactive.messaging.cloudevents.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for Cloud event Connector
 * Assigned ID range is 15300-15399
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface CloudEventExceptions {

    CloudEventExceptions ex = Messages.getBundle(CloudEventExceptions.class);

    @Message(id = 15300, value = "Invalid message - no payload")
    IllegalArgumentException illegalArgumentInvalidMessage();
}
