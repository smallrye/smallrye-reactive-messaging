package io.smallrye.reactive.messaging.kafka.base;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import io.smallrye.reactive.messaging.kafka.base.KafkaBrokerExtension.KafkaBootstrapServers;
import io.vertx.mutiny.core.Vertx;

@ExtendWith(KafkaBrokerExtension.class)
public class KafkaTestBase extends WeldTestBase {
    public static final int KAFKA_PORT = 9092;

    public Vertx vertx;
    public static KafkaUsage usage;
    public AdminClient adminClient;

    public String topic;

    @BeforeAll
    static void initUsage(@KafkaBootstrapServers String bootstrapServers) {
        usage = new KafkaUsage(bootstrapServers);
    }

    @BeforeEach
    public void createVertxAndInitUsage() {
        vertx = Vertx.vertx();
    }

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        topic = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    public void closeVertx() {
        if (vertx != null) {
            vertx.closeAndAwait();
        }
    }

    @AfterEach
    public void closeAdminClient() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    public KafkaMapBasedConfig kafkaConfig() {
        return kafkaConfig("");
    }

    public KafkaMapBasedConfig kafkaConfig(String prefix) {
        return kafkaConfig(prefix, false);
    }

    public KafkaMapBasedConfig kafkaConfig(String prefix, boolean tracing) {
        return new KafkaMapBasedConfig(prefix, tracing).put("bootstrap.servers", usage.getBootstrapServers());
    }

    public KafkaMapBasedConfig newCommonConfigForSource() {
        String randomId = UUID.randomUUID().toString();
        return kafkaConfig().build(
                "group.id", randomId,
                "key.deserializer", StringDeserializer.class.getName(),
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "tracing-enabled", false,
                "topic", topic,
                "graceful-shutdown", false,
                "channel-name", topic);
    }

    public void createTopic(String topic, int partition) {
        try {
            getOrCreateAdminClient().createTopics(Collections.singletonList(new NewTopic(topic, partition, (short) 1)))
                    .all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets) {
        try {
            getOrCreateAdminClient().alterConsumerGroupOffsets(groupId, topicPartitionOffsets).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String groupId,
            List<TopicPartition> topicPartitions) {
        try {
            return getOrCreateAdminClient().listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions()
                    .topicPartitions(topicPartitions)).partitionsToOffsetAndMetadata().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    AdminClient getOrCreateAdminClient() {
        if (adminClient == null) {
            adminClient = AdminClient.create(Collections.singletonMap(BOOTSTRAP_SERVERS_CONFIG, usage.getBootstrapServers()));
        }
        return adminClient;
    }

}
