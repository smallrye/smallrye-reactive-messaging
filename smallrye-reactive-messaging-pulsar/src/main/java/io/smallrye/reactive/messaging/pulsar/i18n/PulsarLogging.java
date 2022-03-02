package io.smallrye.reactive.messaging.pulsar.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for Pulsar Connector
 * Assigned ID range is 19000-19099
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface PulsarLogging extends BasicLogger {

    PulsarLogging log = Logger.getMessageLogger(PulsarLogging.class, "io.smallrye.reactive.messaging.pulsar");

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19000, value = "Unable to close Pulsar consumer")
    void unableToCloseConsumer(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19001, value = "Unable to close Pulsar producer")
    void unableToCloseProducer(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19002, value = "Unable to close Pulsar client")
    void unableToCloseClient(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19003, value = "Unable to acknowledge message")
    void unableToAcknowledgeMessage(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 19004, value = "Unable to dispatch message to Pulsar")
    void unableToDispatch(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19010, value = "No `subscription-name` set in the configuration, generate a random name: %s")
    void noSubscriptionName(String randomName);

}
