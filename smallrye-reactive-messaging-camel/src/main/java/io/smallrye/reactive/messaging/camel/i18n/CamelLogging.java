package io.smallrye.reactive.messaging.camel.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for Camel Connector
 * Assigned ID range is 17800-17899
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface CamelLogging extends BasicLogger {

    CamelLogging log = Logger.getMessageLogger(CamelLogging.class, "io.smallrye.reactive.messaging.camel");

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17800, value = "Creating publisher from Camel stream named %s")
    void creatingPublisherFromStream(String name);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17801, value = "Creating publisher from Camel endpoint %s")
    void creatingPublisherFromEndpoint(String name);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17802, value = "Creating subscriber from Camel stream named %s")
    void creatingSubscriberFromStream(String name);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17803, value = "Creating subscriber from Camel endpoint %s")
    void creatingSubscriberFromEndpoint(String name);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17804, value = "Exchange failed")
    void exchangeFailed(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 17805, value = "A message sent to channel `%s` has been nacked, ignored failure is: %s.")
    void messageNackedIgnore(String channel, String reason);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17806, value = "The full ignored failure is")
    void messageNackedFullIgnored(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17807, value = "A message sent to channel `%s` has been nacked, fail-stop")
    void messageNackedFailStop(String channel);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17808, value = "The Camel Reactive Stream Service is already defined, skipping configuration")
    void camelReactiveStreamsServiceAlreadyDefined();
}
