package io.smallrye.reactive.messaging.mqtt.server.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface MqttServerMessages {

    MqttServerMessages msg = Messages.getBundle(MqttServerMessages.class);

    // 17400-17499 (messaging)
}