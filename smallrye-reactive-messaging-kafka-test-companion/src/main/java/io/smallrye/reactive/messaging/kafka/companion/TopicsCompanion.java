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

    /**
     * Wait for topic. Waits at most the duration of the given kafkaApiTimeout, with 10 retries.
     *
     * @param topics topic names
     * @throws IllegalStateException if the topic is not found at the end of the timeout or retries
     * @return the Uni of the {@link TopicDescription} for the created topic
     */
    public Uni<Map<String, TopicDescription>> waitForTopics(String... topics) {
        return waitForTopics(Arrays.asList(topics));
    }

    /**
     * Wait for topic. Waits at most the duration of the given kafkaApiTimeout, with 10 retries.
     *
     * @param topics topic names
     * @throws IllegalStateException if the topic is not found at the end of the timeout or retries
     * @return the Uni of the map of topic names to their {@link TopicDescription} for the created topics
     */
    public Uni<Map<String, TopicDescription>> waitForTopics(Collection<String> topics) {
        int retries = 10;
        Duration maxBackOff = kafkaApiTimeout.dividedBy(retries);
        return toUni(() -> adminClient.describeTopics(topics).allTopicNames())
                .onFailure().retry().withBackOff(maxBackOff, maxBackOff).atMost(retries)
                .onFailure().recoverWithUni(e -> Uni.createFrom().failure(new IllegalStateException(
                        "Max number of attempts reached, topics " + topics + " were not created after 10 attempts",
                        e)));
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
        return describe(Arrays.asList(topics));
    }

    /**
     * @param topics topics to describe
     * @return the map of topic names to topic descriptions
     */
    public Map<String, TopicDescription> describe(Collection<String> topics) {
        if (topics.isEmpty()) {
            return describeAll();
        }
        return toUni(() -> adminClient.describeTopics(topics).allTopicNames()).await().atMost(kafkaApiTimeout);
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

    /**
     * Delete topics and wait for deletion
     *
     * @param topics the topic names to delete
     */
    public void deleteAndWait(String... topics) {
        deleteAndWait(Arrays.asList(topics));
    }

    /**
     * Delete topics and wait for deletion
     *
     * @param topics the collection of topic names to delete
     */
    public void deleteAndWait(Collection<String> topics) {
        deleteAndWait(topics, kafkaApiTimeout);
    }

    /**
     * Delete topics and wait for deletion
     *
     * @param topics the collection of topic names to delete
     * @param timeout timeout for topics to be deleted
     */
    public void deleteAndWait(Collection<String> topics, Duration timeout) {
        delete(topics);
        waitForTopicDeletion(topics).await().atMost(timeout);
    }

    /**
     * Wait for topic deletion. Waits at most the duration of the given kafkaApiTimeout, with 10 retries.
     *
     * @param topics topic names to wait for deletion
     * @throws IllegalStateException if the topics are still present at the end of the timeout or retries
     * @return the Uni completing when topics are deleted
     */
    public Uni<Set<String>> waitForTopicDeletion(Collection<String> topics) {
        int retries = 10;
        Duration maxBackOff = kafkaApiTimeout.dividedBy(retries);
        return toUni(() -> adminClient.listTopics().names())
                .onItem().transform(names -> {
                    names.retainAll(topics);
                    return names;
                })
                .repeat().withDelay(maxBackOff)
                .whilst(remaining -> !remaining.isEmpty())
                .select().last().toUni()
                .onFailure().recoverWithUni(e -> Uni.createFrom().failure(new IllegalStateException(
                        "Max number of attempts reached, the topics " + topics + " were not deleted", e)));
    }

    /**
     * Reset topics: delete and recreate them with the same number of partitions.
     *
     * @param topics the topic names to reset
     */
    public void reset(String... topics) {
        reset(Arrays.asList(topics));
    }

    /**
     * Reset topics: delete and recreate them with the same number of partitions.
     *
     * @param topics the collection of topic names to reset
     */
    public void reset(Collection<String> topics) {
        Set<String> toReset = new HashSet<>(topics);
        toReset.retainAll(list());
        if (!toReset.isEmpty()) {
            Map<String, TopicDescription> existingTopics = describe(toReset);
            deleteAndWait(toReset);
            create(existingTopics.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().partitions().size())));
            waitForTopics(toReset).await().atMost(kafkaApiTimeout);
        }
    }

}
