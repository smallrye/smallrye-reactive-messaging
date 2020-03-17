package io.smallrye.reactive.messaging.kafka.impl;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class KafkaSource<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
    private final PublisherBuilder<? extends Message<?>> source;
    private final KafkaConsumer<K, V> consumer;

    public KafkaSource(Vertx vertx, Config config, String servers) {
        Map<String, String> kafkaConfiguration = new HashMap<>();

        String group = config.getOptionalValue(ConsumerConfig.GROUP_ID_CONFIG, String.class).orElseGet(() -> {
            String s = UUID.randomUUID().toString();
            LOGGER.warn("No `group.id` set in the configuration, generate a random id: {}", s);
            return s;
        });

        JsonHelper.asJsonObject(config).forEach(e -> kafkaConfiguration.put(e.getKey(), e.getValue().toString()));
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, group);

        if (!kafkaConfiguration.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            LOGGER.info("Setting {} to {}", ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!kafkaConfiguration.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            LOGGER.info("Key deserializer omitted, using String as default");
            kafkaConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }

        kafkaConfiguration.remove("channel-name");
        kafkaConfiguration.remove("topic");
        kafkaConfiguration.remove("connector");
        kafkaConfiguration.remove("retry");
        kafkaConfiguration.remove("retry-attempts");
        kafkaConfiguration.remove("broadcast");

        this.consumer = KafkaConsumer.create(vertx, kafkaConfiguration);
        String topic = getTopicOrFail(config);

        Objects.requireNonNull(topic, "The topic must be set, or the name must be set");

        Multi<KafkaConsumerRecord<K, V>> multi = consumer.toMulti()
                .onFailure().invoke(t -> LOGGER.error("Unable to read a record from Kafka topic '{}'", topic, t));

        if (config.getOptionalValue("retry", Boolean.class).orElse(true)) {
            int max = config.getOptionalValue("retry-attempts", Integer.class).orElse(-1);
            int retryMaxWait = config.getOptionalValue("retry-max-wait", Integer.class).orElse(30);

            if (max == -1) {
                // always retry
                multi
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(retryMaxWait))
                        .atMost(Long.MAX_VALUE);
            } else {
                multi = multi
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(retryMaxWait))
                        .atMost(max);
            }
        }

        if (config.getOptionalValue("broadcast", Boolean.class).orElse(false)) {
            multi = multi.broadcast().toAllSubscribers();
        }

        this.source = ReactiveStreams.fromPublisher(
                multi
                        .on().subscribed(s -> {
                            // The Kafka subscription must happen on the subscription.
                            this.consumer.subscribeAndAwait(topic);
                        }))
                .map(rec -> new IncomingKafkaRecord<>(consumer, rec));
    }

    public PublisherBuilder<? extends Message<?>> getSource() {
        return source;
    }

    public void closeQuietly() {
        try {
            this.consumer.closeAndAwait();
        } catch (Throwable e) {
            LOGGER.debug("An exception has been caught while closing the Kafka consumer", e);
        }
    }

    private String getTopicOrFail(Config config) {
        return config.getOptionalValue("topic", String.class)
                .orElseGet(
                        () -> config.getOptionalValue("channel-name", String.class)
                                .orElseThrow(() -> new IllegalArgumentException("Topic attribute must be set")));
    }
}
