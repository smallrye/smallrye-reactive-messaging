package io.smallrye.reactive.messaging.eventbus.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for EventBus Connector
 * Assigned ID range is 16700-16799
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface EventBusMessages {

    EventBusMessages msg = Messages.getBundle(EventBusMessages.class);
}