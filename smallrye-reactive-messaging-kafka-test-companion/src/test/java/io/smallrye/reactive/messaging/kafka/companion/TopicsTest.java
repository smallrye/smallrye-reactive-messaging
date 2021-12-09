package io.smallrye.reactive.messaging.kafka.companion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class TopicsTest extends KafkaCompanionTestBase {

    @Test
    void testCreateTopicAndWait() {
        String topicName = companion.topics().createAndWait(topic, 4);

        Map<String, TopicDescription> topicDescriptions = companion.topics().describe();
        assertThat(topicDescriptions).containsKey(topicName)
                .extractingByKey(topicName)
                .extracting(TopicDescription::partitions)
                .satisfies(partitions -> assertThat(partitions).hasSize(4));
    }

    @Test
    void testCreateTopic() {
        companion.topics().create(topic, 1);
        await().until(() -> companion.getOrCreateAdminClient()
                .listTopics().names().get(3, TimeUnit.SECONDS)
                .contains(topic));
    }

    @Test
    void testDescribeTopics() {
        Set<String> currentTopics = companion.topics().list();
        companion.topics().delete(currentTopics);
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 3);
        topics.put("topic2", 2);
        topics.put("topic3", 1);
        companion.topics().create(topics);

        assertThat(companion.topics().list()).contains("topic1", "topic2", "topic3");

        assertThat(companion.topics().describe()).containsKeys("topic1", "topic2", "topic3")
                .hasEntrySatisfying("topic1", t -> assertThat(t.partitions()).hasSize(3))
                .hasEntrySatisfying("topic2", t -> assertThat(t.partitions()).hasSize(2))
                .hasEntrySatisfying("topic3", t -> assertThat(t.partitions()).hasSize(1));
    }
}
