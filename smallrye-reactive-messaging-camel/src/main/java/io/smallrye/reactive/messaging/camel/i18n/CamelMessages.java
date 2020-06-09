package io.smallrye.reactive.messaging.camel.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for Camel Connector
 * Assigned ID range is 17700-17799
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface CamelMessages {

    CamelMessages msg = Messages.getBundle(CamelMessages.class);
}