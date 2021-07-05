package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;
import io.vertx.core.Context;

public class ReactiveKafkaProducer<K, V> implements io.smallrye.reactive.messaging.kafka.KafkaProducer<K, V> {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Producer<K, V> producer;

    private final ExecutorService kafkaWorker;

    private final Map<String, Object> kafkaConfiguration;
    private final String channel;
    private final int closetimeout;

    public ReactiveKafkaProducer(KafkaConnectorOutgoingConfiguration config) {
        this(getKafkaProducerConfiguration(config), config.getChannel(), config.getCloseTimeout());
    }

    public ReactiveKafkaProducer(Map<String, Object> kafkaConfiguration, String channel, int closeTimeout) {
        this.kafkaConfiguration = kafkaConfiguration;
        this.channel = channel;
        this.closetimeout = closeTimeout;

        String keySerializerCN = (String) kafkaConfiguration.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        String valueSerializerCN = (String) kafkaConfiguration.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        if (valueSerializerCN == null) {
            throw ex.missingValueSerializer(this.channel, this.channel);
        }

        Serializer<K> keySerializer = createSerializer(keySerializerCN);
        Serializer<V> valueSerializer = createSerializer(valueSerializerCN);

        // Configure the underlying serializers
        configureSerializer(keySerializer, kafkaConfiguration, true);
        configureSerializer(valueSerializer, kafkaConfiguration, false);

        kafkaWorker = Executors.newSingleThreadExecutor(KafkaSendingThread::new);
        producer = new KafkaProducer<>(kafkaConfiguration, keySerializer, valueSerializer);
    }

    private static <T> Serializer<T> createSerializer(String clazz) {
        try {
            return (Serializer<T>) Utils.newInstance(clazz, Serializer.class);
        } catch (ClassNotFoundException e) {
            throw KafkaExceptions.ex.unableToCreateInstance(clazz, e);
        }
    }

    private static void configureSerializer(Serializer<?> serializer, Map<String, Object> config, boolean isKey) {
        try {
            serializer.configure(config, isKey);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public <T> Uni<T> runOnSendingThread(Function<Producer<K, V>, T> action) {
        return Uni.createFrom().item(() -> action.apply(producer))
                .runSubscriptionOn(kafkaWorker);
    }

    @Override
    public Uni<Void> runOnSendingThread(java.util.function.Consumer<Producer<K, V>> action) {
        return Uni.createFrom().<Void> item(() -> {
            action.accept(producer);
            return null;
        }).runSubscriptionOn(kafkaWorker);
    }

    @Override
    public Uni<RecordMetadata> send(ProducerRecord<K, V> record) {
        return Uni.createFrom().<RecordMetadata> emitter(em -> {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    if (record.topic() != null) {
                        log.unableToWrite(this.channel, record.topic(), exception);
                    } else {
                        log.unableToWrite(this.channel, exception);
                    }
                    em.fail(exception);
                } else {
                    em.complete(metadata);
                }
            });
        }).runSubscriptionOn(kafkaWorker);
    }

    @Override
    public Uni<Void> flush() {
        return runOnSendingThread(producer -> {
            producer.flush();
        });
    }

    @Override
    public Uni<List<PartitionInfo>> partitionsFor(String topic) {
        return runOnSendingThread(producer -> {
            return producer.partitionsFor(topic);
        });
    }

    private static Map<String, Object> getKafkaProducerConfiguration(KafkaConnectorOutgoingConfiguration configuration) {
        Map<String, Object> map = new HashMap<>();
        JsonHelper.asJsonObject(configuration.config())
                .forEach(e -> map.put(e.getKey(), e.getValue().toString()));

        // Acks must be a string, even when "1".
        map.put(ProducerConfig.ACKS_CONFIG, configuration.getAcks());

        if (!map.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.configServers(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
            map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        }

        if (!map.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            log.keySerializerOmitted();
            map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, configuration.getKeySerializer());
        }

        if (!map.containsKey(ProducerConfig.CLIENT_ID_CONFIG)) {
            String id = "kafka-producer-" + configuration.getChannel();
            log.setKafkaProducerClientId(id);
            map.put(ProducerConfig.CLIENT_ID_CONFIG, id);
        }

        if (!map.containsKey(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // If no backoff is set, use 10s, it avoids high load on disconnection.
            map.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }

        ConfigurationCleaner.cleanupProducerConfiguration(map);

        return map;
    }

    public String get(String attribute) {
        return (String) kafkaConfiguration.get(attribute);
    }

    @Override
    public Producer<K, V> unwrap() {
        return producer;
    }

    public Map<String, ?> configuration() {
        return kafkaConfiguration;
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            int timeout = this.closetimeout;
            Uni<Void> uni = runOnSendingThread(p -> {
                p.close(Duration.ofMillis(timeout));
            }).onItem().invoke(kafkaWorker::shutdown);

            if (Context.isOnEventLoopThread()) {
                // We can't block, just forget the result
                uni.subscribeAsCompletionStage();
            } else {
                uni.await().atMost(Duration.ofMillis(timeout * 2L));
            }
        }
    }

    boolean isClosed() {
        return closed.get();
    }
}
