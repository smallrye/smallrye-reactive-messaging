package io.smallrye.reactive.messaging.cloudevents.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for Cloud event Connector
 * Assigned ID range is 15400-15499
 */
@MessageBundle(projectCode = "SRMSG")
public interface CloudEventMessages {

    CloudEventMessages msg = Messages.getBundle(CloudEventMessages.class);
}