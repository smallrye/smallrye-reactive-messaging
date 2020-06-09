package io.smallrye.reactive.messaging.amqp.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for AMQP Connector
 * Assigned ID range is 16000-16099
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface AMQPExceptions {

    AMQPExceptions ex = Messages.getBundle(AMQPExceptions.class);

    @Message(id = 16000, value = "Cannot find a %s bean named %s")
    IllegalStateException illegalStateFindingBean(String className, String beanName);

    @Message(id = 16001, value = "Unable to create a client, probably a config error")
    IllegalStateException illegalStateUnableToCreateClient(@Cause Throwable t);

    @Message(id = 16002, value = "Invalid failure strategy: %s")
    IllegalArgumentException illegalArgumentInvalidFailureStrategy(String strategy);

    @Message(id = 16003, value = "AMQP Connection disconnected")
    IllegalStateException illegalStateConnectionDisconnected();

    @Message(id = 16004, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownFailureStrategy(String strategy);

    @Message(id = 16005, value = "Only one subscriber allowed")
    IllegalStateException illegalStateOnlyOneSubscriberAllowed();

}
