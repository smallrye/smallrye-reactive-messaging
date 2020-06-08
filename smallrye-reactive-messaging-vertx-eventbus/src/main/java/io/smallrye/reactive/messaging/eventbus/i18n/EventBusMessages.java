package io.smallrye.reactive.messaging.eventbus.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface EventBusMessages {

    EventBusMessages msg = Messages.getBundle(EventBusMessages.class);

    // 16700-16799 (String messages)

    @Message(id = 16700, value = "Vert.x instance must not be `null`")
    String vertXInstanceMustNotBeNull();
}