package io.smallrye.reactive.messaging.eventbus.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface EventBusExceptions {

    EventBusExceptions ex = Messages.getBundle(EventBusExceptions.class);

    // 16600-16699 (exceptions)

    @Message(id = 16600, value = "Cannot enable `publish` and `expect-reply` at the same time")
    IllegalArgumentException illegalArgumentPublishAndExpectReply();

}
