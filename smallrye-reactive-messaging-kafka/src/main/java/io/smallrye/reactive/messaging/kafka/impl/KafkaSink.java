package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
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

    private final KafkaWriteStream<?, ?> stream;
    private final int partition;
    private final String key;
    private final String topic;
    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;
    private final long retries;

    public KafkaSink(Vertx vertx, KafkaConnectorOutgoingConfiguration config) {
        JsonObject kafkaConfiguration = extractProducerConfiguration(config);

        stream = KafkaWriteStream.create(vertx.getDelegate(), kafkaConfiguration.getMap());
        stream.exceptionHandler(log::unableToWrite);

        partition = config.getPartition();
        retries = config.getRetries();
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
                .onError(log::unableToDispatch)
                .ignore();
    }

    private Function<Message<?>, Uni<Void>> writeMessageToKafka() {
        return message -> {
            try {
                Optional<OutgoingKafkaRecordMetadata<?>> om = getOutgoingKafkaRecordMetadata(message);
                OutgoingKafkaRecordMetadata<?> metadata = om.orElse(null);
                String actualTopic = metadata == null || metadata.getTopic() == null ? this.topic : metadata.getTopic();
                if (actualTopic == null) {
                    log.ignoringNoTopicSet();
                    return Uni.createFrom().item(() -> null);
                }

                ProducerRecord<?, ?> record = getProducerRecord(message, metadata, actualTopic);
                log.sendingMessageToTopic(message, actualTopic);

                //noinspection unchecked,rawtypes
                Uni<Void> uni = Uni.createFrom()
                        .emitter(
                                e -> stream.send((ProducerRecord) record, ar -> handleWriteResult(ar, message, record, e)));

                if (this.retries > 0) {
                    uni = uni.onFailure().retry()
                            .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(20)).atMost(this.retries);
                }
                return uni
                        .onFailure().recoverWithUni(t -> {
                            // Log and nack the messages on failure.
                            log.nackingMessage(message, actualTopic, t);
                            return Uni.createFrom().completionStage(message.nack(t));
                        });
            } catch (RuntimeException e) {
                log.unableToSendRecord(e);
                return Uni.createFrom().failure(e);
            }
        };
    }

    private void handleWriteResult(AsyncResult<?> ar, Message<?> message, ProducerRecord<?, ?> record,
            UniEmitter<? super Void> emitter) {
        String actualTopic = record.topic();
        if (ar.succeeded()) {
            log.successfullyToTopic(message, actualTopic);
            message.ack().whenComplete((x, f) -> {
                if (f != null) {
                    emitter.fail(f);
                } else {
                    emitter.complete(null);
                }
            });
        } else {
            // Fail, there will be retry.
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
            log.configServers(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
            kafkaConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        }

        if (!kafkaConfiguration.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            log.keyDeserializerOmitted();
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
                    log.errorWhileClosingWriteStream(ar.cause());
                }
                latch.countDown();
            });
        } catch (Throwable e) {
            log.errorWhileClosingWriteStream(e);
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
                Subscriptions.fail(subscriber, ex.illegalStateOnlyOneSubscriber());
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
                throw ex.illegalStateConsumeWithoutBackPressure();
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
