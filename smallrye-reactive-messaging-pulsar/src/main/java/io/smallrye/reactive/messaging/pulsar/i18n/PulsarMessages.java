package io.smallrye.reactive.messaging.pulsar.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for GCP Pub/Sub Connector
 * Assigned ID range is 19200-19299
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface PulsarMessages {

    PulsarMessages msg = Messages.getBundle(PulsarMessages.class);

    @Message(id = 19200, value = "%s must be non-null")
    String mustNotBeNull(String fieldName);

    @Message(id = 19201, value = "%s is required")
    String isRequired(String fieldName);

}
