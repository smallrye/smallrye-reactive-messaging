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

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 19011, value = "Created consumer for channel `%s` with schema '%s' and configuration: %s")
    void createdConsumerWithConfig(String channel, String schema, Object consumerConf);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 19012, value = "Created producer for channel `%s` with schema '%s' configuration: %s")
    void createdProducerWithConfig(String channel, String schema, Object producerConf);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19013, value = "A message sent to channel `%s` has been nacked, fail-stopping the processing")
    void messageNackedFailStop(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19014, value = "A message sent to channel `%s` has been nacked, ignored failure is: %s.")
    void messageNackedIgnored(String channel, String reason);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 19015, value = "The full ignored failure is")
    void messageNackedFullIgnored(@Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 19016, value = "The consumer for channel `%s` failed to receive message")
    void failedToReceiveFromConsumer(String channel, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 19017, value = "The consumer for channel `%s` reached end of topic")
    void consumerReachedEndOfTopic(String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 19018, value = "The client for channel `%s` has been closed")
    void clientClosed(String channel, @Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19019, value = "Unable to parse redelivery backoff config `%s` for channel `%s`")
    void unableToParseRedeliveryBackoff(String redeliveryBackoff, String channel);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 19020, value = "Created client with configuration: %s")
    void createdClientWithConfig(Object clientConf);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 19021, value = "No schema type %s found for the channel %s")
    void primitiveSchemaNotFound(String schemaName, String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19022, value = "The schema provider not found with id '%s', for channel '%s' falling back to default schema %s")
    void schemaProviderNotFound(String schemaProviderId, String channel, String defaultSchema);
}
