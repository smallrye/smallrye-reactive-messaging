package io.smallrye.reactive.messaging.eventbus.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for EventBus Connector
 * Assigned ID range is 16600-16699
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface EventBusExceptions {

    EventBusExceptions ex = Messages.getBundle(EventBusExceptions.class);

    @Message(id = 16600, value = "Cannot enable `publish` and `expect-reply` at the same time")
    IllegalArgumentException illegalArgumentPublishAndExpectReply();

}
