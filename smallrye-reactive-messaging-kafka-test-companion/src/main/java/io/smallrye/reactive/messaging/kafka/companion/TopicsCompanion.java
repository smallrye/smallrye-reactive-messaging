package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.toUni;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import io.smallrye.mutiny.Uni;

/**
 * Companion for Topics operations on Kafka broker
 */
public class TopicsCompanion {

    private final AdminClient adminClient;
    private final Duration kafkaApiTimeout;

    public TopicsCompanion(AdminClient adminClient, Duration kafkaApiTimeout) {
        this.adminClient = adminClient;
        this.kafkaApiTimeout = kafkaApiTimeout;
    }

    /**
     * @param newTopics the set of {@link NewTopic}s to create
     */
    public void create(Collection<NewTopic> newTopics) {
        toUni(() -> adminClient.createTopics(newTopics).all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param topicPartitions the map of topic names to partition counts to create
     */
    public void create(Map<String, Integer> topicPartitions) {
        create(topicPartitions.entrySet().stream()
                .map(e -> new NewTopic(e.getKey(), e.getValue(), (short) 1))
                .collect(Collectors.toList()));
    }

    /**
     * Create topic
     *
     * @param topic the topic name
     * @param partition the partition count
     */
    public void create(String topic, int partition) {
        create(Collections.singletonList(new NewTopic(topic, partition, (short) 1)));
    }

    /**
     * Create topic and wait for creation
     *
     * @param topic the topic name
     * @param partition the partition count
     * @return the name of the created topic
     */
    public String createAndWait(String topic, int partition) {
        return createAndWait(topic, partition, kafkaApiTimeout).name();
    }

    /**
     * Create topic and wait for creation
     *
     * @param topic the topic name
     * @param partition the partition count
     * @param timeout timeout for topic to be created
     * @return the description of the created topic
     */
    public TopicDescription createAndWait(String topic, int partition, Duration timeout) {
        create(topic, partition);
        return waitForTopic(topic).await().atMost(timeout);
    }

    /**
     * Wait for topic. Waits at most the duration of the given kafkaApiTimeout, with 10 retries.
     *
     * @param topic name
     * @throws IllegalStateException if the topic is not found at the end of the timeout or retries
     * @return the Uni of the {@link TopicDescription} for the created topic
     */
    public Uni<TopicDescription> waitForTopic(String topic) {
        int retries = 10;
        Duration maxBackOff = kafkaApiTimeout.dividedBy(retries);
        return toUni(() -> adminClient.describeTopics(Collections.singletonList(topic)).allTopicNames())
                .onFailure().retry().withBackOff(maxBackOff, maxBackOff).atMost(retries)
                .onItem().transform(m -> m.get(topic))
                .onFailure().recoverWithUni(e -> Uni.createFrom().failure(new IllegalStateException(
                        "Max number of attempts reached, the topic " + topic + " was not created after 10 attempts", e)));
    }

    boolean checkIfTheTopicIsCreated(String topic, Map<String, TopicDescription> description) {
        if (description == null) {
            return false;
        }
        TopicDescription td = description.get(topic);
        if (td == null) {
            return false;
        }
        // The topic is created, check that each partition has a leader
        List<TopicPartitionInfo> partitions = td.partitions();
        for (TopicPartitionInfo partition : partitions) {
            if (partition.leader() == null || partition.leader().id() < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return the set of topic names
     */
    public Set<String> list() {
        return toUni(() -> adminClient.listTopics().names()).await().atMost(kafkaApiTimeout);
    }

    /**
     * @return the map of topic names to topic descriptions
     */
    public Map<String, TopicDescription> describeAll() {
        return toUni(() -> adminClient.listTopics().names())
                .onItem().transformToUni(topics -> toUni(() -> adminClient.describeTopics(topics).allTopicNames()))
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param topics topics to describe
     * @return the map of topic names to topic descriptions
     */
    public Map<String, TopicDescription> describe(String... topics) {
        if (topics.length == 0) {
            return describeAll();
        }
        return toUni(() -> adminClient.describeTopics(Arrays.asList(topics)).allTopicNames()).await().atMost(kafkaApiTimeout);
    }

    /**
     * Deletes records from given topics
     *
     * @param topics the topic names to clear
     */
    public void clear(String... topics) {
        toUni(() -> adminClient.describeTopics(Arrays.asList(topics)).allTopicNames())
                .map(m -> m.values().stream()
                        .flatMap(t -> t.partitions().stream().map(p -> new TopicPartition(t.name(), p.partition())))
                        .collect(Collectors.toMap(t -> t, t -> RecordsToDelete.beforeOffset(-1))))
                .chain(m -> toUni(() -> adminClient.deleteRecords(m).all()))
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * Same as {@link #clear(String...)} but does not throw an exception if a topic doesn't exist.
     *
     * @param topics the topic names to clear
     */
    public void clearIfExists(String... topics) {
        clearIfExists(Set.of(topics));
    }

    /**
     * Same as {@link #clear(String...)} but does not throw an exception if a topic doesn't exist.
     *
     * @param topics the collection of topic names to clear
     */
    public void clearIfExists(Collection<String> topics) {
        Set<String> toDelete = new HashSet<>(topics);
        toDelete.retainAll(list());
        if (!toDelete.isEmpty()) {
            clear(toDelete.toArray(new String[0]));
        }
    }

    /**
     * @param topics the collection of topic names to delete
     */
    public void delete(Collection<String> topics) {
        toUni(() -> adminClient.deleteTopics(topics).all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param topics the topic names to delete
     */
    public void delete(String... topics) {
        delete(Arrays.asList(topics));
    }
}
