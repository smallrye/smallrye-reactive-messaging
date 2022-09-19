package io.smallrye.reactive.messaging.jms.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for JMS Connector
 * Assigned ID range is 15600-15699
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface JmsExceptions {

    JmsExceptions ex = Messages.getBundle(JmsExceptions.class);

    @Message(id = 15600, value = "Unable to load the class")
    IllegalArgumentException illegalStateUnableToLoadClass(@Cause Throwable t);

    @Message(id = 15601, value = "Unable to unwrap message to %s")
    IllegalArgumentException illegalStateUnableToUnwrap(Class unwrapType);

    @Message(id = 15602, value = "Cannot find a jakarta.jms.ConnectionFactory bean")
    IllegalStateException illegalStateCannotFindFactory();

    @Message(id = 15603, value = "Cannot find a jakarta.jms.ConnectionFactory bean named %s")
    IllegalStateException illegalStateCannotFindNamedFactory(String factoryName);

    @Message(id = 15604, value = "Unknown session mode: %s")
    IllegalArgumentException illegalStateUnknowSessionMode(String mode);

    @Message(id = 15605, value = "The key must not be `null` or blank")
    IllegalArgumentException illegalStateKeyNull();

    @Message(id = 15606, value = "The value must not be `null`")
    IllegalArgumentException illegalStateValueNull();

    @Message(id = 15607, value = "Invalid delivery mode, it should be either `persistent` or `non_persistent`: %s")
    IllegalArgumentException illegalArgumentInvalidDeliveryMode(String v);

    @Message(id = 15608, value = "Invalid destination type, it should be either `queue` or `topic`: %s")
    IllegalArgumentException illegalArgumentInvalidDestinationType(String replyToDestinationType);

    @Message(id = 15609, value = "Unable to map JMS properties to the outgoing message, OutgoingJmsProperties expected, found %s")
    IllegalStateException illegalStateUnableToMapProperties(String name);

    @Message(id = 15610, value = "Unknown destination type: %s")
    IllegalStateException illegalStateUnknownDestinationType(String type);

    @Message(id = 15611, value = "Invalid destination, only topic can be durable")
    IllegalArgumentException illegalArgumentInvalidDestination();

    @Message(id = 15612, value = "Unknown destination type: %s")
    IllegalArgumentException illegalArgumentUnknownDestinationType(String type);

    @Message(id = 15613, value = "There is already a subscriber")
    IllegalStateException illegalStateAlreadySubscriber();

}
