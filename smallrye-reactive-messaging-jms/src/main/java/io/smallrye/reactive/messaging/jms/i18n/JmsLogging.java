package io.smallrye.reactive.messaging.jms.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for JMS Connector
 * Assigned ID range is 15800-15899
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface JmsLogging extends BasicLogger {

    JmsLogging log = Logger.getMessageLogger(JmsLogging.class, "io.smallrye.reactive.messaging.jms");

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 15800, value = "Unable to send message to JMS")
    void unableToSend(@Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15801, value = "Creating queue %s")
    void creatingQueue(String name);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15802, value = "Creating topic %s")
    void creatingTopic(String name);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 15803, value = "Unable to receive JMS messages - client has been closed")
    void clientClosed();
}