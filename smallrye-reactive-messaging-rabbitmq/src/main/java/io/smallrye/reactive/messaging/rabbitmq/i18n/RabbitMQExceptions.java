package io.smallrye.reactive.messaging.rabbitmq.i18n;

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

    @Message(id = 16000, value = "Cannot find a %s bean named %s")
    IllegalStateException illegalStateFindingBean(String className, String beanName);

    @Message(id = 16001, value = "Expecting downstream to consume without back-pressure")
    IllegalStateException illegalStateConsumeWithoutBackPressure();

    @Message(id = 16002, value = "Invalid failure strategy: %s")
    IllegalArgumentException illegalArgumentInvalidFailureStrategy(String strategy);

    @Message(id = 16003, value = "AMQP Connection disconnected")
    IllegalStateException illegalStateConnectionDisconnected();

    @Message(id = 16004, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownFailureStrategy(String strategy);

    @Message(id = 16005, value = "Only one subscriber allowed")
    IllegalStateException illegalStateOnlyOneSubscriberAllowed();

    @Message(id = 16006, value = "The value of max-inflight-messages must be greater than 0")
    IllegalArgumentException illegalArgumentInvalidMaxInflightMessages();

    @Message(id = 16007, value = "If specified, the value of default-ttl must be greater than or equal to 0")
    IllegalArgumentException illegalArgumentInvalidDefaultTtl();

    @Message(id = 16008, value = "If specified, the value of queue.ttl must be greater than or equal to 0")
    IllegalArgumentException illegalArgumentInvalidQueueTtl();

    @Message(id = 16009, value = "Unable to create a client, probably a config error")
    IllegalStateException illegalStateUnableToCreateClient(@Cause Throwable t);
}
