package io.smallrye.reactive.messaging.mqtt.server.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for MQTT Server Connector
 * Assigned ID range is 17300-17399
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface MqttServerExceptions {

    MqttServerExceptions ex = Messages.getBundle(MqttServerExceptions.class);
}