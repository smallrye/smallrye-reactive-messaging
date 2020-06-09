package io.smallrye.reactive.messaging.mqtt.server.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for MQTT Server Connector
 * Assigned ID range is 17400-17499
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface MqttServerMessages {

    MqttServerMessages msg = Messages.getBundle(MqttServerMessages.class);
}