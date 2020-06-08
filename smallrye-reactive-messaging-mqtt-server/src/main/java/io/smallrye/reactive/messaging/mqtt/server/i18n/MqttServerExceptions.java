package io.smallrye.reactive.messaging.mqtt.server.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface MqttServerExceptions {

    MqttServerExceptions ex = Messages.getBundle(MqttServerExceptions.class);

    // 17300-17399 (exceptions)
}