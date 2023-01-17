package io.smallrye.reactive.messaging.kafka.i18n;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
    void sendingMessageToTopic(org.eclipse.microprofile.reactive.messaging.Message<?> message, String topic);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18210, value = "Unable to send a record to Kafka ")
    void unableToSendRecord(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18211, value = "Message %s sent successfully to Kafka topic-partition '%s-%d', with offset %d")
    void successfullyToTopic(org.eclipse.microprofile.reactive.messaging.Message<?> message, String topic, int partition,
            long offset);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18212, value = "Message %s was not sent to Kafka topic '%s' - nacking message")
    void nackingMessage(org.eclipse.microprofile.reactive.messaging.Message<?> message, String topic, @Cause Throwable t);

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
    @Message(id = 18222, value = "Unable to execute consumer revoked re-balance listener for group '%s'")
    void unableToExecuteConsumerRevokedRebalanceListener(String consumerGroup, @Cause Throwable t);

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
    @Message(id = 18231, value = "The record %d from topic-partition '%s' has waited for %s seconds to be acknowledged." +
            " This waiting time is greater than the configured threshold (%d ms). At the moment %d messages from this" +
            " partition are awaiting acknowledgement. The last committed offset for this partition was %d. This error" +
            " is due to a potential issue in the application which does not acknowledged the records in a timely fashion." +
            " The connector cannot commit as a record processing has not completed.")
    void waitingForAckForTooLong(long offset, TopicPartition topicPartition, long delay, long configInMs, long queueSize,
            long lastCommittedOffset);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18232, value = "Will commit for group '%s' every %d milliseconds.")
    void settingCommitInterval(String group, long commitInterval);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18233, value = "Invalid value serializer to write a structured Cloud Event. Found %s, expected the org.apache.kafka.common.serialization.StringSerializer")
    void invalidValueSerializerForStructuredCloudEvent(String serializer);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18234, value = "Auto-commit disabled for channel %s")
    void disableAutoCommit(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18235, value = "Will not health check throttled commit strategy for group '%s'.")
    void disableThrottledCommitStrategyHealthCheck(String group);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18236, value = "Will mark throttled commit strategy for group '%s' as unhealthy if records go more than %d milliseconds without being processed.")
    void setThrottledCommitStrategyReceivedRecordMaxAge(String group, long unprocessedRecordMaxAge);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18237, value = "Setting client.id for Kafka producer to %s")
    void setKafkaProducerClientId(String name);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18239, value = "Received acknowledgement for record %d on '%s' (consumer group: '%s'). Ignoring it " +
            "because the partition is not assigned to the consume anymore. Record will likely be processed again. Current assignments are %s.")
    void acknowledgementFromRevokedTopicPartition(long offset, TopicPartition topicPartition, String groupId,
            Collection<TopicPartition> assignments);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18240, value = "'%s' commit strategy used for channel '%s'")
    void commitStrategyForChannel(String strategy, String channel);

    @LogMessage(level = Logger.Level.FATAL)
    @Message(id = 18241, value = "The deserialization failure handler `%s` throws an exception")
    void deserializationFailureHandlerFailure(String instance, @Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18243, value = "Shutting down - Pausing all topic-partitions")
    void pauseAllPartitionOnTermination();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18244, value = "Shutting down - Waiting for message processing to complete, %d messages still in processing")
    void waitingForMessageProcessing(long p);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18245, value = "There are still %d unprocessed messages after the closing timeout")
    void messageStillUnprocessedAfterTimeout(long unprocessed);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18246, value = "Pausing Kafka consumption for client %s (%s), queue size %s >= %s")
    void pausingChannel(String channel, String clientId, int queueSize, int maxQueueSize);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18247, value = "Resuming Kafka consumption for channel %s (%s), queue size %s <= %s")
    void resumingChannel(String channel, String clientId, int queueSize, int halfMaxQueueSize);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18248, value = "Key serializer omitted, using String as default")
    void keySerializerOmitted();

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18249, value = "Unable to recover from the deserialization failure (topic: %s), configure a DeserializationFailureHandler to recover from errors.")
    void unableToDeserializeMessage(String topic, @Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18250, value = "The configuration property '%s' is deprecated and is replaced by '%s'.")
    void deprecatedConfig(String deprecated, String replace);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18251, value = "Committed %s")
    void committed(Map<TopicPartition, OffsetAndMetadata> offsets);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18252, value = "Failed to commit %s, it will be re-attempted")
    void failedToCommit(Map<TopicPartition, OffsetAndMetadata> offsets, @Cause Throwable failure);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18253, value = "Removing topic-partition '%s' from the store - the partition is not assigned to the consumer anymore. Current assignments are: %s")
    void removingPartitionFromStore(TopicPartition tp, Collection<TopicPartition> assignments);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18254, value = "Topic-partition '%s' has been revoked - going to commit offset %d")
    void partitionRevokedCollectingRecordsToCommit(TopicPartition partition, long commit);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18255, value = "Received a record from topic-partition '%s' at offset %d, while the last committed offset is %d - Ignoring record")
    void receivedOutdatedOffset(TopicPartition topicPartition, long offset, long lastCommitted);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18256, value = "Initialize record store for topic-partition '%s' at position %d.")
    void initializeStoreAtPosition(TopicPartition topicPartition, long position);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18257, value = "Kafka consumer %s, connected to Kafka brokers '%s', belongs to the '%s' consumer group and is configured to poll records from %s")
    void connectedToKafka(String id, String bootstrapServers, String consumerGroup, Set<String> topics);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18258, value = "Kafka producer %s, connected to Kafka brokers '%s', is configured to write records to '%s'")
    void connectedToKafka(String id, String bootstrapServers, String topic);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18259, value = "Kafka latest commit strategy failed to commit record from topic-partition '%s' at offset %d")
    void failedToCommitAsync(TopicPartition topicPartition, long offset);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18260, value = "Unable to recover from the serialization failure (topic: %s), configure a SerializationFailureHandler to recover from errors.")
    void unableToSerializeMessage(String topic, @Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18261, value = "Unable to initialize producer from channel %s.")
    void unableToInitializeProducer(String channel, @Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18262, value = "Aborting transaction for producer id %s in channel %s.")
    void transactionAborted(String producerId, String channel);

    @LogMessage(level = Logger.Level.FATAL)
    @Message(id = 18263, value = "The serialization failure handler `%s` throws an exception")
    void serializationFailureHandlerFailure(String instance, @Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18264, value = "No `checkpoint.state-store` given to use with checkpoint commit strategy. `file` will be used.")
    void checkpointDefaultStateStore();

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18265, value = "Will not health check checkpoint commit strategy for consumer '%s'.")
    void disableCheckpointCommitStrategyHealthCheck(String consumerId);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18266, value = "Will mark checkpoint commit strategy for consumer '%s' as unhealthy if processing state go more than %d milliseconds without being persisted.")
    void setCheckpointCommitStrategyUnsyncedStateMaxAge(String consumerId, int unsyncedStateMaxAge);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18267, value = "Partitions assigned to client %s : %s with initial state %s")
    void checkpointPartitionsAssigned(String consumerId, Collection<TopicPartition> assignments, String state);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18268, value = "Failed fetching state on partitions assigned to client %s : %s")
    void failedCheckpointPartitionsAssigned(String consumerId, Collection<TopicPartition> assignments, @Cause Throwable cause);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18269, value = "Partitions revoked from client %s: %s with state to persist %s")
    void checkpointPartitionsRevoked(String consumerId, Collection<TopicPartition> assignments, String state);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18270, value = "Persisted state for client %s : %s")
    void checkpointPersistedState(String consumerId, String state);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18271, value = "Failed persisting state for %s : %s")
    void checkpointFailedPersistingState(String consumerId, String state, @Cause Throwable cause);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18272, value = "Failed persisting state but can be retried for %s : %s")
    void checkpointFailedPersistingStateRetryable(String consumerId, String state, @Cause Throwable cause);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18273, value = "Checkpoint commit strategy processing state type not found for channel %s : %s")
    void checkpointStateTypeNotFound(String channel, String fqcn);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18274, value = "Error caught in producer interceptor `onSend` for channel %s")
    void interceptorOnSendError(String channel, @Cause Throwable cause);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 18275, value = "Error caught in producer interceptor `onAcknowledge` for channel %s")
    void interceptorOnAcknowledgeError(String channel, @Cause Throwable cause);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 18276, value = "Error caught in producer interceptor `close` for channel %s")
    void interceptorCloseError(String channel, @Cause Throwable cause);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18277, value = "Delayed retry topics configured for channel `%s` with topics `%s`, max retries, `%d`, timeout `%d`ms, dlq topic `%s`")
    void delayedRetryTopic(String channel, Collection<String> retryTopics, int maxRetries, long timeout,
            String deadLetterTopic);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18278, value = "A message sent to channel `%s` has been nacked, sending the record to topic %s")
    void delayedRetryNack(String channel, String topic);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18279, value = "A message sent to channel `%s` reached delayed retry timeout `%s` milliseconds reached for record `%s`")
    void delayedRetryTimeout(String channel, long timeout, String record);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18280, value = "A message sent to channel `%s` has been nacked and won't be retried again. Configure `dead-letter-queue.topic` for sending the record to a dead letter topic")
    void delayedRetryNoDlq(String channel);

}
