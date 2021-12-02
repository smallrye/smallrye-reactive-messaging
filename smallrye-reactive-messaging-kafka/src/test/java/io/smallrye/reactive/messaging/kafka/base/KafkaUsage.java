package io.smallrye.reactive.messaging.kafka.base;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.smallrye.reactive.messaging.kafka.tracing.HeaderExtractAdapter;
import io.strimzi.test.container.StrimziKafkaContainer;

/**
 * Simplify the usage of a Kafka client.
 */
public class KafkaUsage implements AutoCloseable {
    public static final int KAFKA_PORT = 9092;

    private final static Logger LOGGER = Logger.getLogger(KafkaUsage.class);
    private final String brokers;
    private AdminClient adminClient;

    public KafkaUsage(String bootstrapServers) {
        this.brokers = bootstrapServers;
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    /**
     * We need to restart the broker on the same exposed port.
     * Test Containers makes this unnecessarily complicated, but well, let's go for a hack.
     * See https://github.com/testcontainers/testcontainers-java/issues/256.
     *
     * @param kafka the broker that will be closed
     * @param gracePeriodInSecond number of seconds to wait before restarting
     * @return the new broker
     */
    public static StrimziKafkaContainer restart(StrimziKafkaContainer kafka, int gracePeriodInSecond) {
        int port = kafka.getMappedPort(KAFKA_PORT);
        try {
            kafka.close();
        } catch (Exception e) {
            // Ignore me.
        }
        await().until(() -> !kafka.isRunning());
        sleep(Duration.ofSeconds(gracePeriodInSecond));

        return startKafkaBroker(port);
    }

    public static StrimziKafkaContainer startKafkaBroker(int port) {
        StrimziKafkaContainer kafka = KafkaBrokerExtension.createKafkaContainer().withPort(port);
        kafka.start();
        await().until(kafka::isRunning);
        return kafka;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static String getHeader(Headers headers, String key) {
        return new String(headers.lastHeader(key).value(), StandardCharsets.UTF_8);
    }

    public Properties getConsumerProperties(String groupId, String clientId, OffsetResetStrategy autoOffsetReset) {
        if (groupId == null) {
            throw new IllegalArgumentException("The groupId is required");
        } else {
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", brokers);
            props.setProperty("group.id", "usage-" + groupId);
            props.setProperty("enable.auto.commit", Boolean.FALSE.toString());
            if (autoOffsetReset != null) {
                props.setProperty("auto.offset.reset",
                        autoOffsetReset.toString().toLowerCase());
            }

            if (clientId != null) {
                props.setProperty("client.id", "usage-" + clientId);
            }

            return props;
        }
    }

    public Properties getProducerProperties(String clientId) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("acks", Integer.toString(1));
        if (clientId != null) {
            props.setProperty("client.id", "usage-" + clientId);
        }

        return props;
    }

    /**
     * Use the supplied function to asynchronously produce messages and write them to the cluster.
     *
     * @param producerName the name of the producer; may not be null
     * @param messageCount the number of messages to produce; must be positive
     * @param keySerializer the serializer for the keys; may not be null
     * @param valueSerializer the serializer for the values; may not be null
     * @param completionCallback the function to be called when the producer is completed; may be null
     * @param messageSupplier the function to produce messages; may not be null
     */
    public <K, V> void produce(String producerName, int messageCount,
            Serializer<K> keySerializer, Serializer<V> valueSerializer,
            Runnable completionCallback,
            Supplier<ProducerRecord<K, V>> messageSupplier) {
        Properties props = getProducerProperties(producerName);
        Thread t = new Thread(() -> {
            LOGGER.infof("Starting producer %s to write %s messages", producerName, messageCount);
            try (KafkaProducer<K, V> producer = new KafkaProducer<>(props, keySerializer, valueSerializer)) {
                for (int i = 0; i != messageCount; ++i) {
                    ProducerRecord<K, V> record = messageSupplier.get();
                    producer.send(record);
                    producer.flush();
                    LOGGER.infof("Producer %s: sent message %s", producerName, record);
                }
            } finally {
                if (completionCallback != null) {
                    completionCallback.run();
                }
                LOGGER.infof("Stopping producer %s", producerName);
            }
        });
        t.setName(producerName + "-thread");
        t.start();
    }

    public void produceIntegers(int messageCount, Runnable completionCallback,
            Supplier<ProducerRecord<String, Integer>> messageSupplier) {
        Serializer<String> keySer = new StringSerializer();
        Serializer<Integer> valSer = new IntegerSerializer();
        String randomId = UUID.randomUUID().toString();
        this.produce(randomId, messageCount, keySer, valSer, completionCallback, messageSupplier);
    }

    public void produceDoubles(int messageCount, Runnable completionCallback,
            Supplier<ProducerRecord<String, Double>> messageSupplier) {
        Serializer<String> keySer = new StringSerializer();
        Serializer<Double> valSer = new DoubleSerializer();
        String randomId = UUID.randomUUID().toString();
        this.produce(randomId, messageCount, keySer, valSer, completionCallback, messageSupplier);
    }

    public void produceStrings(int messageCount, Runnable completionCallback,
            Supplier<ProducerRecord<String, String>> messageSupplier) {
        Serializer<String> keySer = new StringSerializer();
        Serializer<String> valSer = new StringSerializer();
        String randomId = UUID.randomUUID().toString();
        this.produce(randomId, messageCount, keySer, valSer, completionCallback, messageSupplier);
    }

    /**
     * Use the supplied function to asynchronously consume messages from the cluster.
     *
     * @param groupId the name of the group; may not be null
     * @param clientId the name of the client; may not be null
     * @param autoOffsetReset how to pick a starting offset when there is no initial offset in ZooKeeper or if an offset is
     *        out of range; may be null for the default to be used
     * @param keyDeserializer the deserializer for the keys; may not be null
     * @param valueDeserializer the deserializer for the values; may not be null
     * @param continuation the function that determines if the consumer should continue; may not be null
     * @param offsetCommitCallback the callback that should be used after committing offsets; may be null if offsets are
     *        not to be committed
     * @param completion the function to call when the consumer terminates; may be null
     * @param topics the set of topics to consume; may not be null or empty
     * @param consumerFunction the function to consume the messages; may not be null
     */
    public <K, V> void consume(String groupId, String clientId, OffsetResetStrategy autoOffsetReset,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
            BooleanSupplier continuation, OffsetCommitCallback offsetCommitCallback, Runnable completion,
            Collection<String> topics,
            java.util.function.Consumer<ConsumerRecord<K, V>> consumerFunction) {
        Properties props = getConsumerProperties(groupId, clientId, autoOffsetReset);
        Thread t = new Thread(() -> {
            LOGGER.infof("Starting consumer %s to read messages", clientId);
            try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer)) {
                consumer.subscribe(new ArrayList<>(topics));
                while (continuation.getAsBoolean()) {
                    consumer.poll(Duration.ofMillis(10)).forEach(record -> {
                        LOGGER.infof("Consumer %s: consuming message %s", clientId, record);
                        consumerFunction.accept(record);
                        if (offsetCommitCallback != null) {
                            consumer.commitAsync(offsetCommitCallback);
                        }
                    });
                }
            } finally {
                if (completion != null) {
                    completion.run();
                }
                LOGGER.debugf("Stopping consumer %s", clientId);
            }
        });
        t.setName(clientId + "-thread");
        t.start();
    }

    public <K, V> void consumeCount(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Consumer<ConsumerRecord<K, V>> consumer) {
        String randomId = UUID.randomUUID().toString();
        AtomicLong readCounter = new AtomicLong();
        this.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDeserializer, valueDeserializer,
                this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), null, completion,
                Collections.singleton(topicName), (record) -> {
                    consumer.accept(record);
                    readCounter.incrementAndGet();
                });
    }

    public <K, V> void consumeCount(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, BiConsumer<K, V> consumer) {
        consumeCount(topicName, count, timeout, unit, completion, keyDeserializer, valueDeserializer, record -> {
            consumer.accept(record.key(), record.value());
        });
    }

    public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            BiConsumer<String, String> consumer) {
        consumeCount(topicName, count, timeout, unit, completion, new StringDeserializer(), new StringDeserializer(),
                consumer);
    }

    public void consumeDoubles(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            BiConsumer<String, Double> consumer) {
        consumeCount(topicName, count, timeout, unit, completion, new StringDeserializer(), new DoubleDeserializer(),
                consumer);
    }

    public void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            BiConsumer<String, Integer> consumer) {
        consumeCount(topicName, count, timeout, unit, completion, new StringDeserializer(), new IntegerDeserializer(),
                consumer);
    }

    public void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            Consumer<ConsumerRecord<String, Integer>> consumer) {
        consumeCount(topicName, count, timeout, unit, completion, new StringDeserializer(), new IntegerDeserializer(),
                consumer);
    }

    public void consumeIntegersWithTracing(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            BiConsumer<String, Integer> consumer, Consumer<Context> tracingConsumer) {
        this.consumeIntegers(topicName, count, timeout, unit, completion,
                (record) -> {
                    consumer.accept(record.key(), record.value());
                    tracingConsumer.accept(
                            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                                    .extract(Context.current(), record.headers(), new HeaderExtractAdapter()));
                });
    }

    public void consumeIntegersWithHeaders(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            BiConsumer<String, Integer> consumer, Consumer<Headers> headersConsumer) {
        this.consumeIntegers(topicName, count, timeout, unit, completion,
                (record) -> {
                    consumer.accept(record.key(), record.value());
                    headersConsumer.accept(record.headers());
                });
    }

    protected BooleanSupplier continueIfNotExpired(BooleanSupplier continuation,
            long timeout, TimeUnit unit) {
        return new BooleanSupplier() {
            long stopTime = 0L;

            public boolean getAsBoolean() {
                if (this.stopTime == 0L) {
                    this.stopTime = System.currentTimeMillis() + unit.toMillis(timeout);
                }

                return continuation.getAsBoolean() && System.currentTimeMillis() <= this.stopTime;
            }
        };
    }

    public String createNewTopic(String newTopic, int partitions) {
        createTopic(newTopic, partitions);
        waitForTopic(newTopic);
        return newTopic;
    }

    private void waitForTopic(String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
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

    public String getBootstrapServers() {
        return this.brokers;
    }

    public AdminClient getOrCreateAdminClient() {
        if (adminClient == null) {
            adminClient = AdminClient.create(Collections.singletonMap(BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers()));
        }
        return adminClient;
    }

    public void createTopic(String topic, int partition) {
        try {
            getOrCreateAdminClient().createTopics(Collections.singletonList(new NewTopic(topic, partition, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
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

}
