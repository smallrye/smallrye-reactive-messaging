package io.smallrye.reactive.messaging.cloudevents.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface CloudEventExceptions {

    CloudEventExceptions ex = Messages.getBundle(CloudEventExceptions.class);

    // 15300-15399 (exceptions)

    @Message(id = 15300, value = "Invalid message - no payload")
    IllegalArgumentException illegalArgumentInvalidMessage();
}
