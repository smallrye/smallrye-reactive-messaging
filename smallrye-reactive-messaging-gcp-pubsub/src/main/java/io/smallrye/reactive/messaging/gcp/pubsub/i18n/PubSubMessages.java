package io.smallrye.reactive.messaging.gcp.pubsub.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for GCP Pub/Sub Connector
 * Assigned ID range is 14700-14799
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface PubSubMessages {

    PubSubMessages msg = Messages.getBundle(PubSubMessages.class);

    @Message(id = 14700, value = "%s must be non-null")
    String mustNotBeNull(String fieldName);

    @Message(id = 14701, value = "%s is required")
    String isRequired(String fieldName);

}