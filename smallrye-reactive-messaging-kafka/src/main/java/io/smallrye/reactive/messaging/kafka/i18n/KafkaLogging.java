package io.smallrye.reactive.messaging.kafka.i18n;

import java.util.Set;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for Kafka Connector
 * Assigned ID range is 18200-18299
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface KafkaLogging extends BasicLogger {

    KafkaLogging log = Logger.getMessageLogger(KafkaLogging.class, "io.smallrye.reactive.messaging.kafka");

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18201, value = "Dead queue letter configured with: topic: `%s`, key serializer: `%s`, value serializer: `%s`")
    void deadLetterConfig(String deadQueueTopic, String keySerializer, String valueSerializer);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18202, value = "A message sent to channel `%s` has been nacked, sending the record to a dead letter topic %s")
    void messageNackedDeadLetter(String channel, String topic);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18203, value = "A message sent to channel `%s` has been nacked, fail-stop")
    void messageNackedFailStop(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18204, value = "A message sent to channel `%s` has been nacked, ignored failure is: %s.")
    void messageNackedIgnore(String channel, String reason);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18205, value = "The full ignored failure is")
    void messageNackedFullIgnored(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18206, value = "Unable to write to Kafka from channel %s (topic: %s)")
    void unableToWrite(String channel, String topic, @Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18206, value = "Unable to write to Kafka from channel %s (no topic set)")
    void unableToWrite(String channel, @Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18207, value = "Unable to dispatch message to Kafka")
    void unableToDispatch(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18209, value = "Sending message %s to Kafka topic '%s'")
    void sendingMessageToTopic(org.eclipse.microprofile.reactive.messaging.Message message, String topic);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18210, value = "Unable to send a record to Kafka ")
    void unableToSendRecord(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18211, value = "Message %s sent successfully to Kafka topic '%s'")
    void successfullyToTopic(org.eclipse.microprofile.reactive.messaging.Message message, String topic);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18212, value = "Message %s was not sent to Kafka topic '%s' - nacking message")
    void nackingMessage(org.eclipse.microprofile.reactive.messaging.Message message, String topic, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18213, value = "Setting %s to %s")
    void configServers(String serverConfig, String servers);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18214, value = "Key deserializer omitted, using String as default")
    void keyDeserializerOmitted();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18215, value = "An error has been caught while closing the Kafka Write Stream")
    void errorWhileClosingWriteStream(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18216, value = "No `group.id` set in the configuration, generate a random id: %s")
    void noGroupId(String randomId);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18217, value = "Unable to read a record from Kafka topics '%s'")
    void unableToReadRecord(Set<String> topics, @Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18218, value = "An exception has been caught while closing the Kafka consumer")
    void exceptionOnClose(@Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18219, value = "Loading KafkaConsumerRebalanceListener from configured name '%s'")
    void loadingConsumerRebalanceListenerFromConfiguredName(String configuredName);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18220, value = "Loading KafkaConsumerRebalanceListener from group id '%s'")
    void loadingConsumerRebalanceListenerFromGroupId(String consumerGroup);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18221, value = "Unable to execute consumer assigned re-balance listener for group '%s'. The consumer has been paused. Will retry until the consumer session times out in which case will resume to force a new re-balance attempt.")
    void unableToExecuteConsumerAssignedRebalanceListener(String consumerGroup, @Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18222, value = "Unable to execute consumer revoked re-balance listener for group '%s'")
    void unableToExecuteConsumerRevokedRebalanceListener(String consumerGroup, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18223, value = "Executing consumer assigned re-balance listener for group '%s'")
    void executingConsumerAssignedRebalanceListener(String consumerGroup);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18224, value = "Executing consumer revoked re-balance listener for group '%s'")
    void executingConsumerRevokedRebalanceListener(String consumerGroup);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18225, value = "Executed consumer assigned re-balance listener for group '%s'")
    void executedConsumerAssignedRebalanceListener(String consumerGroup);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18226, value = "Executed consumer revoked re-balance listener for group '%s'")
    void executedConsumerRevokedRebalanceListener(String consumerGroup);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18227, value = "Re-enabling consumer for group '%s'. This consumer was paused because of a re-balance failure.")
    void reEnablingConsumerForGroup(String consumerGroup);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18228, value = "A failure has been reported for Kafka topics '%s'")
    void failureReported(Set<String> topics, @Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18242, value = "A failure has been reported during a rebalance - the operation will be retried: '%s'")
    void failureReportedDuringRebalance(Set<String> topics, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18229, value = "Configured topics for channel '%s': %s")
    void configuredTopics(String channel, Set<String> topics);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18230, value = "Configured topics matching pattern for channel '%s': %s")
    void configuredPattern(String channel, String pattern);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18231, value = "The amount of received messages without acking is too high for topic partition '%s', "
            + "amount %d. The last committed offset was %d. It means that the Kafka connector received Kafka Records that "
            + "have neither be acked nor nacked in a timely fashion. The connector accumulates records in memory, but that "
            + "buffer reached its maximum capacity or the deadline for ack/nack expired. The connector cannot commit as "
            + "a record processing has not completed.")
    void receivedTooManyMessagesWithoutAcking(String topicPartition, long amount, long lastCommittedOffset);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18232, value = "Will commit for group '%s' every %d milliseconds.")
    void settingCommitInterval(String group, long commitInterval);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18233, value = "Invalid value serializer to write a structured Cloud Event. Found %d, expected the org.apache.kafka.common.serialization.StringSerializer")
    void invalidValueSerializerForStructuredCloudEvent(String serializer);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18234, value = "Auto-commit disabled for channel %s")
    void disableAutoCommit(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18235, value = "Will not health check throttled commit strategy for group '%s'.")
    void disableThrottledCommitStrategyHealthCheck(String group);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18236, value = "Will mark throttled commit strategy for group '%s' as unhealthy if records go more than %d milliseconds without being processed.")
    void setThrottledCommitStrategyReceivedRecordMaxAge(String group, long unprocessedRecordMaxAge);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18237, value = "Setting client.id for Kafka producer to %s")
    void setKafkaProducerClientId(String name);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18238, value = "Setting client.id for Kafka consumer to %s")
    void setKafkaConsumerClientId(String name);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18239, value = "Acked record %d on group '%s' was ignored because the topic partition '%s' was revoked for this instance. Record will likely be processed again.")
    void messageAckedForRevokedTopicPartition(long offset, String groupId, String topicPartition);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18240, value = "'%s' commit strategy used for channel '%s'")
    void commitStrategyForChannel(String strategy, String channel);

    @LogMessage(level = Logger.Level.FATAL)
    @Message(id = 18241, value = "The deserialization failure handler `%s` throws an exception")
    void deserializationFailureHandlerFailure(String instance, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18244, value = "Pausing Kafka consumption for channel %s - no request received for %d seconds")
    void pausingChannel(String channel, int time);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18245, value = "Resuming Kafka consumption for channel %s")
    void resumingChannel(String channel);
}
