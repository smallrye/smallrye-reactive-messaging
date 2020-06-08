package io.smallrye.reactive.messaging.kafka.i18n;

import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRMSG")
public interface KafkaLogging {

    // 18667-18999 (logging)

    KafkaLogging log = Logger.getMessageLogger(KafkaLogging.class, "io.smallrye.reactive.messaging.kafka");

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18667, value = "Merging config with %s")
    void mergingConfigWith(Map<String, Object> defaultKafkaCfg);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18668, value = "Dead queue letter configured with: topic: `%s`, key serializer: `%s`, value serializer: `%s`")
    void deadLetterConfig(String deadQueueTopic, String keySerializer, String valueSerializer);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18669, value = "A message sent to channel `%s` has been nacked, sending the record to a dead letter topic %s")
    void messageNackedDeadLetter(String channel, String topic);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18670, value = "A message sent to channel `%s` has been nacked, fail-stop")
    void messageNackedFailStop(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18671, value = "A message sent to channel `%s` has been nacked, ignored failure is: %s.")
    void messageNackedIgnore(String channel, String reason);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18672, value = "The full ignored failure is")
    void messageNackedFullIgnored(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18673, value = "Unable to write to Kafka")
    void unableToWrite(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18674, value = "Unable to dispatch message to Kafka")
    void unableToDispatch(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18675, value = "Ignoring message - no topic set")
    void ignoringNoTopicSet();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18676, value = "Sending message %s to Kafka topic '%s'")
    void sendingMessageToTopic(org.eclipse.microprofile.reactive.messaging.Message message, String topic);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18677, value = "Unable to send a record to Kafka ")
    void unableToSendRecord(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18678, value = "Message %s sent successfully to Kafka topic '%s'")
    void successfullyToTopic(org.eclipse.microprofile.reactive.messaging.Message message, String topic);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18679, value = "Message %s was not sent to Kafka topic '%s' - nacking message")
    void nackingMessage(org.eclipse.microprofile.reactive.messaging.Message message, String topic, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18680, value = "Setting %s to %s")
    void configServers(String serverConfig, String servers);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18681, value = "Key deserializer omitted, using String as default")
    void keyDeserializerOmitted();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18682, value = "An error has been caught while closing the Kafka Write Stream")
    void errorWhileClosingWriteStream(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18683, value = "No `group.id` set in the configuration, generate a random id: %s")
    void noGroupId(String randomId);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18684, value = "Unable to read a record from Kafka topic '%s'")
    void unableToReadRecord(String topic, @Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18685, value = "An exception has been caught while closing the Kafka consumer")
    void exceptionOnClose(@Cause Throwable t);

}
