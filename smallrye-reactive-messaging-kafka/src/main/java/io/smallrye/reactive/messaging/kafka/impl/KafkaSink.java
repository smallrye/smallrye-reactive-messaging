package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.mutiny.core.Vertx;

public class KafkaSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
    private final KafkaWriteStream<?, ?> stream;
    private final int partition;
    private final String key;
    private final String topic;
    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;

    public KafkaSink(Vertx vertx, KafkaConnectorOutgoingConfiguration config) {
        JsonObject kafkaConfiguration = extractProducerConfiguration(config);

        stream = KafkaWriteStream.create(vertx.getDelegate(), kafkaConfiguration.getMap());
        stream.exceptionHandler(t -> LOGGER.error("Unable to write to Kafka", t));

        partition = config.getPartition();
        key = config.getKey().orElse(null);
        topic = config.getTopic().orElseGet(config::getChannel);
        boolean waitForWriteCompletion = config.getWaitForWriteCompletion();
        int maxInflight = config.getMaxInflightMessages();
        if (maxInflight == 5) { // 5 is the Kafka default.
            maxInflight = config.config().getOptionalValue(ProducerConfig.MAX_BLOCK_MS_CONFIG, Integer.class)
                    .orElse(5);
        }
        int inflight = maxInflight;
        subscriber = ReactiveStreams.<Message<?>> builder()
                .via(new KafkaSenderProcessor(inflight, waitForWriteCompletion, writeMessageToKafka()))
                .onError(t -> LOGGER.error("Unable to dispatch message to Kafka", t))
                .ignore();
    }

    private Function<Message<?>, Uni<Void>> writeMessageToKafka() {
        return message -> {
            try {
                Optional<OutgoingKafkaRecordMetadata<?>> om = getOutgoingKafkaRecordMetadata(message);
                OutgoingKafkaRecordMetadata<?> metadata = om.orElse(null);
                String actualTopic = metadata == null || metadata.getTopic() == null ? this.topic : metadata.getTopic();
                if (actualTopic == null) {
                    LOGGER.error("Ignoring message - no topic set");
                    return Uni.createFrom().item(() -> null);
                }

                ProducerRecord<?, ?> record = getProducerRecord(message, metadata, actualTopic);
                LOGGER.debug("Sending message {} to Kafka topic '{}'", message, actualTopic);

                //noinspection unchecked,rawtypes
                return Uni.createFrom()
                        .emitter(e -> stream.write((ProducerRecord) record, ar -> handleWriteResult(ar, message, record, e)));
            } catch (RuntimeException e) {
                LOGGER.error("Unable to send a record to Kafka ", e);
                return Uni.createFrom().failure(e);
            }
        };
    }

    private void handleWriteResult(AsyncResult<Void> ar, Message<?> message, ProducerRecord<?, ?> record,
            UniEmitter<? super Void> emitter) {
        String actualTopic = record.topic();
        if (ar.succeeded()) {
            LOGGER.debug("Message {} sent successfully to Kafka topic '{}'", message, actualTopic);
            message.ack().whenComplete((x, f) -> {
                if (f != null) {
                    emitter.fail(f);
                } else {
                    emitter.complete(null);
                }
            });
        } else {
            LOGGER.error("Message {} was not sent to Kafka topic '{}'", message, actualTopic,
                    ar.cause());
            emitter.fail(ar.cause());
        }
    }

    private Optional<OutgoingKafkaRecordMetadata<?>> getOutgoingKafkaRecordMetadata(Message<?> message) {
        return message.getMetadata(OutgoingKafkaRecordMetadata.class).map(x -> (OutgoingKafkaRecordMetadata<?>) x);
    }

    private ProducerRecord<?, ?> getProducerRecord(Message<?> message, OutgoingKafkaRecordMetadata<?> om,
            String actualTopic) {
        int actualPartition = om == null || om.getPartition() <= -1 ? this.partition : om.getPartition();
        Object actualKey = om == null || om.getKey() == null ? key : om.getKey();

        long actualTimestamp;
        if ((om == null) || (om.getKey() == null)) {
            actualTimestamp = -1;
        } else {
            actualTimestamp = (om.getTimestamp() != null) ? om.getTimestamp().toEpochMilli() : -1;
        }

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

        // Max inflight
        if (!kafkaConfiguration.containsKey(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            kafkaConfiguration.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, config.getMaxInflightMessages());
        }

        kafkaConfiguration.remove("channel-name");
        kafkaConfiguration.remove("topic");
        kafkaConfiguration.remove("connector");
        kafkaConfiguration.remove("partition");
        kafkaConfiguration.remove("key");
        kafkaConfiguration.remove("max-inflight-messages");
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

    private static class KafkaSenderProcessor
            implements Processor<Message<?>, Message<?>>, Subscription {

        private final long inflights;
        private final boolean waitForCompletion;
        private final Function<Message<?>, Uni<Void>> send;
        private final AtomicReference<Subscription> subscription = new AtomicReference<>();
        private final AtomicReference<Subscriber<? super Message<?>>> downstream = new AtomicReference<>();

        public KafkaSenderProcessor(int inflights, boolean waitForCompletion, Function<Message<?>, Uni<Void>> send) {
            this.inflights = inflights;
            this.waitForCompletion = waitForCompletion;
            this.send = send;
        }

        @Override
        public void subscribe(
                Subscriber<? super Message<?>> subscriber) {
            if (!downstream.compareAndSet(null, subscriber)) {
                Subscriptions.fail(subscriber, new IllegalStateException("Only one subscriber allowed"));
            } else {
                if (subscription.get() != null) {
                    subscriber.onSubscribe(this);
                }
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (this.subscription.compareAndSet(null, subscription)) {
                Subscriber<? super Message<?>> subscriber = downstream.get();
                if (subscriber != null) {
                    subscriber.onSubscribe(this);
                }
            } else {
                Subscriber<? super Message<?>> subscriber = downstream.get();
                if (subscriber != null) {
                    subscriber.onSubscribe(Subscriptions.CANCELLED);
                }
            }
        }

        @Override
        public void onNext(Message<?> message) {
            if (waitForCompletion) {
                send.apply(message)
                        .subscribe().with(
                                x -> requestNext(message),
                                this::onError);
            } else {
                send.apply(message)
                        .subscribe().with(x -> {
                        }, this::onError);
                requestNext(message);
            }
        }

        @Override
        public void request(long l) {
            if (l != Long.MAX_VALUE) {
                throw new IllegalStateException("Expecting downstream to consume without back-pressure");
            }
            subscription.get().request(inflights);
        }

        @Override
        public void cancel() {
            Subscription s = KafkaSenderProcessor.this.subscription.getAndSet(Subscriptions.CANCELLED);
            if (s != null) {
                s.cancel();
            }
        }

        private void requestNext(Message<?> message) {
            Subscriber<? super Message<?>> down = downstream.get();
            if (down != null) {
                down.onNext(message);
            }
            Subscription up = this.subscription.get();
            if (up != null) {
                up.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            Subscriber<? super Message<?>> subscriber = downstream.getAndSet(null);
            if (subscriber != null) {
                subscriber.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            Subscriber<? super Message<?>> subscriber = downstream.getAndSet(null);
            if (subscriber != null) {
                subscriber.onComplete();
            }
        }
    }
}
