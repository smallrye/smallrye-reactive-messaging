package io.smallrye.reactive.messaging.connectors.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for In-memory Connector
 * Assigned ID range is 18400-18499
 */
@MessageBundle(projectCode = "SRMSG")
public interface InMemoryMessages {

    InMemoryMessages msg = Messages.getBundle(InMemoryMessages.class);
}