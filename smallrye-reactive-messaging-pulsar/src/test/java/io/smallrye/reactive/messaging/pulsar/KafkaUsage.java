package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaUsage {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaUsage.class);
    private String brokers;

    public KafkaUsage() {
        this.brokers = "localhost:9092";
    }

    public Properties getConsumerProperties(String groupId, String clientId, OffsetResetStrategy autoOffsetReset) {
        if (groupId == null) {
            throw new IllegalArgumentException("The groupId is required");
        } else {
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", brokers);
            props.setProperty("group.id", groupId);
            props.setProperty("enable.auto.commit", Boolean.FALSE.toString());
            if (autoOffsetReset != null) {
                props.setProperty("auto.offset.reset",
                        autoOffsetReset.toString().toLowerCase());
            }

            if (clientId != null) {
                props.setProperty("client.id", clientId);
            }

            return props;
        }
    }

    public Properties getProducerProperties(String clientId) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("acks", Integer.toString(1));
        if (clientId != null) {
            props.setProperty("client.id", clientId);
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
            LOGGER.info("Starting producer {} to write {} messages", producerName, messageCount);
            try (KafkaProducer<K, V> producer = new KafkaProducer<>(props, keySerializer, valueSerializer)) {
                for (int i = 0; i != messageCount; ++i) {
                    ProducerRecord<K, V> record = messageSupplier.get();
                    producer.send(record);
                    producer.flush();
                    LOGGER.info("Producer {}: sent message {}", producerName, record);
                }
            } finally {
                if (completionCallback != null) {
                    completionCallback.run();
                }
                LOGGER.info("Stopping producer {}", producerName);
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
            LOGGER.info("Starting consumer {} to read messages", clientId);
            try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer)) {
                consumer.subscribe(new ArrayList<>(topics));
                while (continuation.getAsBoolean()) {
                    consumer.poll(Duration.ofMillis(10)).forEach(record -> {
                        LOGGER.info("Consumer {}: consuming message {}", clientId, record);
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
                LOGGER.debug("Stopping consumer {}", clientId);
            }
        });
        t.setName(clientId + "-thread");
        t.start();
    }

    private void consumeStrings(BooleanSupplier continuation, Runnable completion, Collection<String> topics,
            Consumer<ConsumerRecord<String, String>> consumerFunction) {
        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();
        OffsetCommitCallback offsetCommitCallback = null;
        this.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, continuation, offsetCommitCallback,
                completion, topics, consumerFunction);
    }

    private void consumeIntegers(BooleanSupplier continuation, Runnable completion, Collection<String> topics,
            Consumer<ConsumerRecord<String, Integer>> consumerFunction) {
        Deserializer<String> keyDes = new StringDeserializer();
        Deserializer<Integer> valDes = new IntegerDeserializer();
        String randomId = UUID.randomUUID().toString();
        OffsetCommitCallback offsetCommitCallback = null;
        this.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, valDes, continuation, offsetCommitCallback,
                completion, topics, consumerFunction);
    }

    public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            BiConsumer<String, String> consumer) {
        AtomicLong readCounter = new AtomicLong();
        this.consumeStrings(this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), completion,
                Collections.singleton(topicName),
                (record) -> {
                    consumer.accept(record.key(), record.value());
                    readCounter.incrementAndGet();
                });
    }

    void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            BiConsumer<String, Integer> consumer) {
        AtomicLong readCounter = new AtomicLong();
        this.consumeIntegers(
                this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit),
                completion,
                Collections.singleton(topicName),
                (record) -> {
                    consumer.accept(record.key(), record.value());
                    readCounter.incrementAndGet();
                });
    }

    void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            Consumer<ConsumerRecord<String, Integer>> consumer) {
        AtomicLong readCounter = new AtomicLong();
        this.consumeIntegers(
                this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit),
                completion,
                Collections.singleton(topicName),
                (record) -> {
                    consumer.accept(record);
                    readCounter.incrementAndGet();
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

}
