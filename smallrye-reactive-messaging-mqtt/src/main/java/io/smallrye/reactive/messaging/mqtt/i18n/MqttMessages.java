package io.smallrye.reactive.messaging.mqtt.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messaging for MQTT Connector
 * Assigned ID range is 17200-17299
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface MqttMessages {

    MqttMessages msg = Messages.getBundle(MqttMessages.class);
}