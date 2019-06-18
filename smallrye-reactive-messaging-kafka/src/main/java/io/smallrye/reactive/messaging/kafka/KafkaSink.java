package io.smallrye.reactive.messaging.kafka;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.reactivex.core.Vertx;

class KafkaSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
    private final KafkaWriteStream stream;
    private final int partition;
    private final String key;
    private final String topic;
    private final boolean waitForWriteCompletion;
    private final SubscriberBuilder<? extends Message, Void> subscriber;

    KafkaSink(Vertx vertx, Config config, String servers) {
        JsonObject kafkaConfiguration = JsonHelper.asJsonObject(config);

        // Acks must be a string, even when "1".
        if (kafkaConfiguration.containsKey(ProducerConfig.ACKS_CONFIG)) {
            kafkaConfiguration.put(ProducerConfig.ACKS_CONFIG,
                    kafkaConfiguration.getValue(ProducerConfig.ACKS_CONFIG).toString());
        }

        if (!kafkaConfiguration.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            LOGGER.info("Setting {} to {}", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            kafkaConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!kafkaConfiguration.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            LOGGER.info("Key deserializer omitted, using String as default");
            kafkaConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }

        stream = KafkaWriteStream.create(vertx.getDelegate(), kafkaConfiguration.getMap());
        stream.exceptionHandler(t -> LOGGER.error("Unable to write to Kafka", t));

        partition = config.getOptionalValue("partition", Integer.class).orElse(-1);
        key = config.getOptionalValue("key", String.class).orElse(null);
        topic = getTopicOrNull(config);
        waitForWriteCompletion = config.getOptionalValue("waitForWriteCompletion", Boolean.class).orElse(true);
        if (topic == null) {
            LOGGER.warn("No default topic configured, only sending messages with an explicit topic set");
        }

        subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(message -> {
                    try {
                        ProducerRecord record;
                        if (message instanceof KafkaMessage) {
                            KafkaMessage km = ((KafkaMessage) message);

                            if (this.topic == null && km.getTopic() == null) {
                                LOGGER.error("Ignoring message - no topic set");
                                return CompletableFuture.completedFuture(message);
                            }

                            Integer actualPartition = null;
                            if (this.partition != -1) {
                                actualPartition = this.partition;
                            }
                            if (km.getPartition() != null) {
                                actualPartition = km.getPartition();
                            }

                            String actualTopicToBeUSed = topic;
                            if (km.getTopic() != null) {
                                actualTopicToBeUSed = km.getTopic();
                            }

                            if (actualTopicToBeUSed == null) {
                                LOGGER.error("Ignoring message - no topic set");
                                return CompletableFuture.completedFuture(message);
                            } else {
                                record = new ProducerRecord<>(
                                        actualTopicToBeUSed,
                                        actualPartition,
                                        km.getTimestamp(),
                                        km.getKey() == null ? this.key : km.getKey(),
                                        km.getPayload(),
                                        km.getHeaders().unwrap());
                                LOGGER.info("Sending message {} to Kafka topic '{}'", message, record.topic());
                            }
                        } else {
                            if (this.topic == null) {
                                LOGGER.error("Ignoring message - no topic set");
                                return CompletableFuture.completedFuture(message);
                            }
                            if (partition == -1) {
                                record = new ProducerRecord<>(topic, null, null, key, message.getPayload());
                            } else {
                                record = new ProducerRecord<>(topic, partition, null, key, message.getPayload());
                            }
                        }

                        CompletableFuture<Message> future = new CompletableFuture<>();
                        Handler<AsyncResult<RecordMetadata>> handler = ar -> {
                            if (ar.succeeded()) {
                                LOGGER.info("Message {} sent successfully to Kafka topic '{}'", message, record.topic());
                                future.complete(message);
                            } else {
                                LOGGER.error("Message {} was not sent to Kafka topic '{}'", message, record.topic(),
                                        ar.cause());
                                future.completeExceptionally(ar.cause());
                            }
                        };
                        CompletableFuture<? extends Message<?>> result = future.thenCompose(x -> message.ack())
                                .thenApply(x -> message);
                        stream.write(record, handler);
                        if (waitForWriteCompletion) {
                            return result;
                        } else {
                            return CompletableFuture.completedFuture(message);
                        }

                    } catch (RuntimeException e) {
                        LOGGER.error("Unable to send a record to Kafka ", e);
                        return CompletableFuture.completedFuture(message);
                    }
                })
                .onError(t -> LOGGER.error("Unable to dispatch message to Kafka", t))
                .ignore();

    }

    SubscriberBuilder<? extends Message, Void> getSink() {
        return subscriber;
    }

    void close() {
        this.stream.close();
    }

    private String getTopicOrNull(Config config) {
        return config.getOptionalValue("topic", String.class)
                .orElseGet(
                        () -> config.getOptionalValue("channel-name", String.class).orElse(null));
    }
}
