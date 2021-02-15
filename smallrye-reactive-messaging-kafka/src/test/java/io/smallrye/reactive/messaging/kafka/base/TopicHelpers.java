package io.smallrye.reactive.messaging.kafka.base;

import static io.smallrye.reactive.messaging.kafka.base.KafkaBrokerExtension.getBootstrapServers;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class TopicHelpers {

    public static String createNewTopic(String newTopic, int partitions) {
        try (
                AdminClient adminClient = KafkaAdminClient.create(
                        Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers()))) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(newTopic, partitions, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        waitForTopic(newTopic);
        return newTopic;
    }

    private static void waitForTopic(String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            int maxRetries = 10;
            boolean done = false;
            for (int i = 0; i < maxRetries && !done; i++) {
                List<PartitionInfo> partitionInfo = producer.partitionsFor(topic);
                done = !partitionInfo.isEmpty();
                for (PartitionInfo info : partitionInfo) {
                    if (info.leader() == null || info.leader().id() < 0) {
                        done = false;
                    }
                }
            }
            assertTrue("Timed out waiting for topic", done);
        }
    }
}
