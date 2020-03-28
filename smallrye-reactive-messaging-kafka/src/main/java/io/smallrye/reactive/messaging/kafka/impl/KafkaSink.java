package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.mutiny.core.Vertx;

public class KafkaSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
    private final KafkaWriteStream<?, ?> stream;
    private final int partition;
    private final String key;
    private final String topic;
    private final boolean waitForWriteCompletion;
    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;

    @SuppressWarnings("rawtypes")
    public KafkaSink(Vertx vertx, KafkaConnectorOutgoingConfiguration config) {
        JsonObject kafkaConfiguration = extractProducerConfiguration(config);

        stream = KafkaWriteStream.create(vertx.getDelegate(), kafkaConfiguration.getMap());
        stream.exceptionHandler(t -> LOGGER.error("Unable to write to Kafka", t));

        partition = config.getPartition();
        key = config.getKey().orElse(null);
        topic = config.getTopic().orElseGet(config::getChannel);
        waitForWriteCompletion = config.getWaitForWriteCompletion();
        subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(message -> {
                    try {
                        Optional<OutgoingKafkaRecordMetadata> om = message.getMetadata(OutgoingKafkaRecordMetadata.class);
                        OutgoingKafkaRecordMetadata<?> metadata = om.orElse(null);
                        String actualTopic = metadata == null || metadata.getTopic() == null ? this.topic : metadata.getTopic();
                        if (actualTopic == null) {
                            LOGGER.error("Ignoring message - no topic set");
                            return CompletableFuture.completedFuture(message);
                        }

                        ProducerRecord record = getProducerRecord(message, metadata, actualTopic);
                        LOGGER.debug("Sending message {} to Kafka topic '{}'", message, record.topic());

                        CompletableFuture<Message> future = new CompletableFuture<>();
                        Handler<AsyncResult<Void>> handler = ar -> {
                            if (ar.succeeded()) {
                                LOGGER.debug("Message {} sent successfully to Kafka topic '{}'", message, record.topic());
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

    @SuppressWarnings("rawtypes")
    private ProducerRecord getProducerRecord(Message<?> message, OutgoingKafkaRecordMetadata<?> om,
            String actualTopic) {
        int actualPartition = om == null || om.getPartition() <= -1 ? this.partition : om.getPartition();
        Object actualKey = om == null || om.getKey() == null ? key : om.getKey();
        long actualTimestamp = om == null || om.getKey() == null ? -1
                : om.getTimestamp() != null ? om.getTimestamp().toEpochMilli() : -1;
        Iterable<Header> kafkaHeaders = om == null || om.getHeaders() == null ? Collections.emptyList() : om.getHeaders();
        return new ProducerRecord<>(
                actualTopic,
                actualPartition == -1 ? null : actualPartition,
                actualTimestamp == -1L ? null : actualTimestamp,
                actualKey,
                message.getPayload(),
                kafkaHeaders);
    }

    private JsonObject extractProducerConfiguration(KafkaConnectorOutgoingConfiguration config) {
        JsonObject kafkaConfiguration = JsonHelper.asJsonObject(config.config());

        // Acks must be a string, even when "1".
        kafkaConfiguration.put(ProducerConfig.ACKS_CONFIG, config.getAcks());

        if (!kafkaConfiguration.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            LOGGER.info("Setting {} to {}", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
            kafkaConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        }

        if (!kafkaConfiguration.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            LOGGER.info("Key deserializer omitted, using String as default");
            kafkaConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getKeySerializer());
        }

        kafkaConfiguration.remove("channel-name");
        kafkaConfiguration.remove("topic");
        kafkaConfiguration.remove("connector");
        kafkaConfiguration.remove("partition");
        kafkaConfiguration.remove("key");
        return kafkaConfiguration;
    }

    public SubscriberBuilder<? extends Message<?>, Void> getSink() {
        return subscriber;
    }

    public void closeQuietly() {
        CountDownLatch latch = new CountDownLatch(1);
        try {
            this.stream.close(ar -> {
                if (ar.failed()) {
                    LOGGER.debug("An error has been caught while closing the Kafka Write Stream", ar.cause());
                }
                latch.countDown();
            });
        } catch (Throwable e) {
            LOGGER.debug("An error has been caught while closing the Kafka Write Stream", e);
            latch.countDown();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
