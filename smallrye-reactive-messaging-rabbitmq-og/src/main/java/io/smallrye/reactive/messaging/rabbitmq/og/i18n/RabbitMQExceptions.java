package io.smallrye.reactive.messaging.rabbitmq.og.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Defines a bundle of exception messages each with a unique id.
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface RabbitMQExceptions {
    RabbitMQExceptions ex = Messages.getBundle(RabbitMQExceptions.class);

    @Message(id = 19000, value = "Cannot find a %s bean named %s")
    IllegalStateException illegalStateFindingBean(String className, String beanName);

    @Message(id = 19001, value = "Expecting downstream to consume without back-pressure")
    IllegalStateException illegalStateConsumeWithoutBackPressure();

    @Message(id = 19002, value = "Invalid failure strategy: %s")
    IllegalArgumentException illegalArgumentInvalidFailureStrategy(String strategy);

    @Message(id = 19003, value = "RabbitMQ Connection disconnected")
    IllegalStateException illegalStateConnectionDisconnected();

    @Message(id = 19004, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownFailureStrategy(String strategy);

    @Message(id = 19005, value = "Only one subscriber allowed")
    IllegalStateException illegalStateOnlyOneSubscriberAllowed();

    @Message(id = 19006, value = "The value of max-inflight-messages must be greater than 0")
    IllegalArgumentException illegalArgumentInvalidMaxInflightMessages();

    @Message(id = 19007, value = "If specified, the value of default-ttl must be greater than or equal to 0")
    IllegalArgumentException illegalArgumentInvalidDefaultTtl();

    @Message(id = 19008, value = "If specified, the value of queue.ttl must be greater than or equal to 0")
    IllegalArgumentException illegalArgumentInvalidQueueTtl();

    @Message(id = 19009, value = "Unable to create a client, probably a config error")
    IllegalStateException illegalStateUnableToCreateClient(@Cause Throwable t);

    @Message(id = 19010, value = "Unable to create RabbitMQ channel")
    IllegalStateException illegalStateUnableToCreateChannel(@Cause Throwable t);

    @Message(id = 19011, value = "RabbitMQ connection is closed")
    IllegalStateException illegalStateConnectionClosed();

    @Message(id = 19012, value = "RabbitMQ channel is closed")
    IllegalStateException illegalStateChannelClosed();
}
