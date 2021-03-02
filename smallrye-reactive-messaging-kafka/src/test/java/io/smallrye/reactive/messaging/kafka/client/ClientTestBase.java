package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.TopicHelpers;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ClientTestBase extends KafkaTestBase {

    public static final int DEFAULT_TEST_TIMEOUT = 60_000;

    protected String topic;
    protected final int partitions = 4;
    protected long receiveTimeoutMillis = DEFAULT_TEST_TIMEOUT;
    protected final long requestTimeoutMillis = 3000;
    protected final long sessionTimeoutMillis = 12000;

    private final List<List<Integer>> expectedMessages = new ArrayList<>(partitions);
    protected final List<List<Integer>> receivedMessages = new ArrayList<>(partitions);

    public Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMillis));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    protected MapBasedConfig createConsumerConfig(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put("channel-name", "test");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMillis));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(3000));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new MapBasedConfig(props);
    }

    protected ProducerRecord<Integer, String> createProducerRecord(int index, boolean expectSuccess) {
        int partition = index % partitions;
        if (expectSuccess) {
            expectedMessages.get(partition).add(index);
        }
        return new ProducerRecord<>(topic, partition, index, "Message " + index);
    }

    protected Multi<ProducerRecord<Integer, String>> createProducerRecords(int count) {
        return Multi.createFrom().range(0, count).map(i -> createProducerRecord(i, true));
    }

    protected void onReceive(IncomingKafkaRecord<Integer, String> record) {
        receivedMessages.get(record.getPartition()).add(record.getKey());
    }

    protected void checkConsumedMessages() {
        assertThat(receivedMessages).hasSize(expectedMessages.size());
        assertThat(receivedMessages).containsExactlyElementsOf(expectedMessages);
    }

    protected void checkConsumedMessages(int receiveStartIndex, int receiveCount) {
        for (int i = 0; i < partitions; i++) {
            checkConsumedMessages(i, receiveStartIndex, receiveStartIndex + receiveCount - 1);
        }
    }

    protected void checkConsumedMessages(int partition, int receiveStartIndex, int receiveEndIndex) {
        List<Integer> expected = new ArrayList<>(expectedMessages.get(partition));
        expected.removeIf(index -> index < receiveStartIndex || index > receiveEndIndex);
        assertThat(receivedMessages.get(partition)).containsExactlyElementsOf(expected);
    }

    protected int count(List<List<Integer>> list) {
        return list.stream().mapToInt(List::size).sum();
    }

    protected void resetMessages() {
        expectedMessages.clear();
        receivedMessages.clear();
        for (int i = 0; i < partitions; i++) {
            expectedMessages.add(new ArrayList<>());
        }
        for (int i = 0; i < partitions; i++) {
            receivedMessages.add(new ArrayList<>());
        }
    }

    protected String createNewTopicWithPrefix(String prefix) {
        String newTopic = prefix + "-" + System.nanoTime();
        TopicHelpers.createNewTopic(newTopic, partitions);
        resetMessages();
        return newTopic;
    }

    protected void clearReceivedMessages() {
        receivedMessages.forEach(List::clear);
    }
}
