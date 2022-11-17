package io.smallrye.reactive.messaging.mqtt.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for MQTT Connector
 * Assigned ID range is 17100-17199
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface MqttLogging extends BasicLogger {

    // 17100-17199 (logging)

    MqttLogging log = Logger.getMessageLogger(MqttLogging.class, "io.smallrye.reactive.messaging.mqtt");

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17100, value = "A message sent to channel `%s` has been nacked, fail-stop")
    void messageNackedFailStop(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 17101, value = "A message sent to channel `%s` has been nacked, ignored failure is: %s.")
    void messageNackedIgnore(String channel, String reason);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17102, value = "The full ignored failure is")
    void messageNackedFullIgnored(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17103, value = "An error has been caught while sending a MQTT message to the broker")
    void errorWhileSendingMessageToBroker(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17104, value = "Ignoring message - no topic set")
    void ignoringNoTopicSet();

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17105, value = "Unable to establish a connection with the MQTT broker")
    void unableToConnectToBroker(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17200, value = "Creating MQTT client from bean named '%s'")
    void createClientFromBean(String optionsBeanName);

}
