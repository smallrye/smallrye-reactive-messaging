package io.smallrye.reactive.messaging.mqtt.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for MQTT Connector
 * Assigned ID range is 17000-17099
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface MqttExceptions {

    MqttExceptions ex = Messages.getBundle(MqttExceptions.class);

    @Message(id = 17000, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownStrategy(String strategy);

    @Message(id = 17001, value = "Invalid QoS value: %s")
    IllegalArgumentException illegalArgumentInvalidQoS(int qos);

    @Message(id = 17002, value = "Cannot find a %s bean identified with %s")
    IllegalStateException illegalStateFindingBean(String className, String beanName);

    @Message(id = 17003, value = "Invalid retain-handling value: %d. Must be 0, 1, or 2.")
    IllegalArgumentException illegalArgumentInvalidRetainHandling(int value);

    @Message(id = 17004, value = "When 'will-flag' is enabled on channel '%s', both 'will-topic' and 'will-payload' must be set")
    IllegalArgumentException illegalArgumentMissingWillTopicOrPayload(String channel);

    @Message(id = 17005, value = "Cannot build a response message: the input message has no MQTT 5 'Response Topic' property")
    IllegalArgumentException illegalArgumentMissingResponseTopic();

}
