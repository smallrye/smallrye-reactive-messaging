package io.smallrye.reactive.messaging.http.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for HTTP Connector
 * Assigned ID range is 16400-16499
 */
@MessageBundle(projectCode = "SRMSG")
public interface HttpMessages {

    HttpMessages msg = Messages.getBundle(HttpMessages.class);

    @Message(id = 16400, value = "Payload must not be null")
    String payloadMustNotBeNull();

}