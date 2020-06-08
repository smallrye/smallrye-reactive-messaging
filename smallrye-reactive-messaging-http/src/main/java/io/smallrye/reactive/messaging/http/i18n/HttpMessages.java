package io.smallrye.reactive.messaging.http.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRRML")
public interface HttpMessages {

    HttpMessages msg = Messages.getBundle(HttpMessages.class);

    // 16400-16499 (String messages)

    @Message(id = 16400, value = "Payload must not be null")
    String payloadMustNotBeNull();

}