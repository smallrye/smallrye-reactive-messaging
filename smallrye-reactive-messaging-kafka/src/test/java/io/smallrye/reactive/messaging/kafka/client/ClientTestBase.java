package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
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

    void sendMessages(int startIndex, int count) throws Exception {
        sendMessages(IntStream.range(0, count).mapToObj(i -> createProducerRecord(startIndex + i, true)));
    }

    void sendMessages(int startIndex, int count, String broker) throws Exception {
        sendMessages(IntStream.range(0, count).mapToObj(i -> new ProducerRecord<>(topic, 0, i, "Message " + i)), broker);
    }

    void sendMessages(Stream<? extends ProducerRecord<Integer, String>> records) throws Exception {
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps())) {
            List<Future<?>> futures = records.map(producer::send).collect(Collectors.toList());

            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        }
    }

    void sendMessages(Stream<? extends ProducerRecord<Integer, String>> records, String broker) throws Exception {
        Map<String, Object> configs = producerProps();
        System.out.println("Sending to broker " + broker);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs)) {
            List<Future<?>> futures = records.map(producer::send).collect(Collectors.toList());
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        }
    }

    public Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put("tracing-enabled", false);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMillis));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public MapBasedConfig createProducerConfig() {
        return new MapBasedConfig(producerProps());
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
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + groupId);
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

    protected void onReceive(IncomingKafkaRecordBatch<Integer, String> record) {
        for (KafkaRecord<Integer, String> kafkaRecord : record) {
            receivedMessages.get(kafkaRecord.getPartition()).add(kafkaRecord.getKey());
        }
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
