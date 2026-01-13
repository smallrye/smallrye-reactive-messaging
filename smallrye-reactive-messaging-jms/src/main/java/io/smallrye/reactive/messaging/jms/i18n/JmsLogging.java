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

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 15804, value = "Terminal error on channel %s")
    void terminalErrorOnChannel(String channelName);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 15805, value = "Terminal error on channel %s. Retries exhausted, No more messages will be received.")
    void terminalErrorRetriesExhausted(String channelName, @Cause Throwable e);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 15806, value = "JMS Exception occurred. Closing the JMS context %s")
    void jmsException(String channelName, @Cause Throwable e);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 15807, value = "A message sent to channel `%s` has been nacked, ignored failure is: %s.")
    void messageNackedIgnore(String channel, String message);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 15808, value = "The full ignored failure is")
    void messageNackedFullIgnored(@Cause Throwable reason);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 15809, value = "A message sent to channel `%s` has been nacked, fail-stop")
    void messageNackedFailStop(String channel);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15810, value = "A message sent to channel `%s` has been nacked, sending the message to a dead letter queue %s")
    void messageNackedDeadLetter(String channel, String dlq);
}
