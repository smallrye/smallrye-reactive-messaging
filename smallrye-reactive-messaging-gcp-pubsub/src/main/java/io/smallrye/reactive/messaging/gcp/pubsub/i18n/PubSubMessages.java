package io.smallrye.reactive.messaging.gcp.pubsub.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRRML")
public interface PubSubMessages {

    PubSubMessages msg = Messages.getBundle(PubSubMessages.class);

    // 14700-14799 (String messages)

    @Message(id = 14700, value = "%s must be non-null")
    String mustNotBeNull(String fieldName);

    @Message(id = 14701, value = "%s is required")
    String isRequired(String fieldName);

}