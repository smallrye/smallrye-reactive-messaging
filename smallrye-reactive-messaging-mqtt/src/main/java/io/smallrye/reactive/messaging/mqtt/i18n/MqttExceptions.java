package io.smallrye.reactive.messaging.mqtt.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface MqttExceptions {

    MqttExceptions ex = Messages.getBundle(MqttExceptions.class);

    // 17000-17099 (exceptions)

    @Message(id = 17000, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownStrategy(String strategy);

}
