package io.smallrye.reactive.messaging.kafka.impl;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailStop;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaIgnoreFailure;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class KafkaSource<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
    private final PublisherBuilder<? extends Message<?>> source;
    private final KafkaConsumer<K, V> consumer;
    private KafkaFailureHandler failureHandler;

    public KafkaSource(Vertx vertx, KafkaConnectorIncomingConfiguration config) {

        Map<String, String> kafkaConfiguration = new HashMap<>();

        String group = config.getGroupId().orElseGet(() -> {
            String s = UUID.randomUUID().toString();
            LOGGER.warn("No `group.id` set in the configuration, generate a random id: {}", s);
            return s;
        });

        JsonHelper.asJsonObject(config.config())
                .forEach(e -> kafkaConfiguration.put(e.getKey(), e.getValue().toString()));
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, group);

        String servers = config.getBootstrapServers();
        if (!kafkaConfiguration.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            LOGGER.info("Setting {} to {}", ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!kafkaConfiguration.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            LOGGER.info("Key deserializer omitted, using String as default");
            kafkaConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        }

        kafkaConfiguration.remove("channel-name");
        kafkaConfiguration.remove("topic");
        kafkaConfiguration.remove("connector");
        kafkaConfiguration.remove("retry");
        kafkaConfiguration.remove("retry-attempts");
        kafkaConfiguration.remove("broadcast");

        this.consumer = KafkaConsumer.create(vertx, kafkaConfiguration);
        String topic = config.getTopic().orElseGet(config::getChannel);

        failureHandler = createFailureHandler(config, vertx, kafkaConfiguration);

        Multi<KafkaConsumerRecord<K, V>> multi = consumer.toMulti()
                .onFailure().invoke(t -> LOGGER.error("Unable to read a record from Kafka topic '{}'", topic, t));

        if (config.getRetry()) {
            int max = config.getRetryAttempts();
            int maxWait = config.getRetryMaxWait();
            if (max == -1) {
                // always retry
                multi
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                        .atMost(Long.MAX_VALUE);
            } else {
                multi = multi
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                        .atMost(max);
            }
        }

        if (config.getBroadcast()) {
            multi = multi.broadcast().toAllSubscribers();
        }

        this.source = ReactiveStreams.fromPublisher(
                multi
                        .on().subscribed(s -> {
                            // The Kafka subscription must happen on the subscription.
                            this.consumer.subscribeAndAwait(topic);
                        }))
                .map(rec -> new IncomingKafkaRecord<>(consumer, rec, failureHandler));
    }

    private KafkaFailureHandler createFailureHandler(KafkaConnectorIncomingConfiguration config, Vertx vertx,
            Map<String, String> kafkaConfiguration) {
        String strategy = config.getFailureStrategy();
        KafkaFailureHandler.Strategy actualStrategy = KafkaFailureHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case FAIL:
                return new KafkaFailStop(LOGGER, config.getChannel());
            case IGNORE:
                return new KafkaIgnoreFailure(LOGGER, config.getChannel());
            case DEAD_LETTER_QUEUE:
                return KafkaDeadLetterQueue.create(LOGGER, vertx, kafkaConfiguration, config);
            default:
                throw new IllegalArgumentException("Invalid failure strategy: " + strategy);
        }

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
}
