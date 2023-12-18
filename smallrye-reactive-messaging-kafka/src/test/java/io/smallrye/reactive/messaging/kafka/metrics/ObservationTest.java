package io.smallrye.reactive.messaging.kafka.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.metrics.ObservationTest.MyReactiveMessagingMessageObservationCollector.KafkaMessageObservation;
import io.smallrye.reactive.messaging.observation.DefaultMessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservationCollector;
import io.smallrye.reactive.messaging.observation.ObservationContext;

public class ObservationTest extends KafkaCompanionTestBase {

    @Test
    void testConsumeIndividualMessages() {
        addBeans(MyReactiveMessagingMessageObservationCollector.class);
        addBeans(MyConsumingApp.class);

        runApplication(kafkaConfig("mp.messaging.incoming.kafka", false)
                .with("topic", topic)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));

        companion.produceStrings()
                .fromMulti(Multi.createFrom().range(0, 5).map(i -> new ProducerRecord<>(topic, null, Integer.toString(i))));

        MyReactiveMessagingMessageObservationCollector reporter = get(MyReactiveMessagingMessageObservationCollector.class);
        await().untilAsserted(() -> {
            assertThat(reporter.getObservations()).hasSize(5);
            assertThat(reporter.getObservations()).allSatisfy(obs -> {
                assertThat(obs.getCreationTime()).isNotEqualTo(-1);
                assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
                assertThat(obs.getReason()).isNull();
            });
        });
    }

    @Test
    void testConsumeBatchMessages() {
        addBeans(MyReactiveMessagingMessageObservationCollector.class);
        addBeans(MyBatchConsumingApp.class);

        runApplication(kafkaConfig("mp.messaging.incoming.kafka", false)
                .with("topic", topic)
                .with("batch", true)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));

        companion.produceStrings()
                .fromMulti(Multi.createFrom().range(0, 1000).map(i -> new ProducerRecord<>(topic, null, Integer.toString(i))));

        MyReactiveMessagingMessageObservationCollector reporter = get(MyReactiveMessagingMessageObservationCollector.class);
        MyBatchConsumingApp batches = get(MyBatchConsumingApp.class);
        await().untilAsserted(() -> {
            assertThat(batches.count()).isEqualTo(1000);
            assertThat(reporter.getObservations()).allSatisfy(obs -> {
                assertThat(obs.getCreationTime()).isNotEqualTo(-1);
                assertThat(obs.getCreationTime()).isNotEqualTo(-1);
                assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
                assertThat(obs.getReason()).isNull();
            });
        });
    }

    @Test
    void testProducer() {
        addBeans(MyReactiveMessagingMessageObservationCollector.class);
        addBeans(MyProducerApp.class);

        runApplication(kafkaConfig("mp.messaging.outgoing.kafka", false)
                .with("topic", topic)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

        MyReactiveMessagingMessageObservationCollector reporter = get(MyReactiveMessagingMessageObservationCollector.class);
        MyProducerApp producer = get(MyProducerApp.class);
        await().untilAsserted(() -> {
            assertThat(producer.count()).isEqualTo(5);
            assertThat(reporter.getObservations()).hasSize(5);
            assertThat(reporter.getObservations()).allSatisfy(obs -> {
                assertThat(obs.getCreationTime()).isNotEqualTo(-1);
                assertThat(obs.getCreationTime()).isNotEqualTo(-1);
                assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
                assertThat(obs.getReason()).isNull();
            });
        });
    }

    @Test
    void testEmitterProducer() {
        addBeans(MyReactiveMessagingMessageObservationCollector.class);
        addBeans(MyEmitterProducerApp.class);

        runApplication(kafkaConfig("mp.messaging.outgoing.kafka", false)
                .with("topic", topic)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

        MyReactiveMessagingMessageObservationCollector reporter = get(MyReactiveMessagingMessageObservationCollector.class);
        MyEmitterProducerApp producer = get(MyEmitterProducerApp.class);
        producer.produce();
        await().untilAsserted(() -> {
            assertThat(producer.count()).isEqualTo(5);
            assertThat(reporter.getObservations()).hasSize(5);
            assertThat(reporter.getObservations()).allSatisfy(obs -> {
                assertThat(obs.getCreationTime()).isNotEqualTo(-1);
                assertThat(obs.getCreationTime()).isNotEqualTo(-1);
                assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
                assertThat(obs.getReason()).isNull();
            });
        });
    }

    @ApplicationScoped
    public static class MyConsumingApp {
        @Incoming("kafka")
        public void consume(String ignored, KafkaMessageObservation metadata) {
            assertThat(metadata).isNotNull();
        }
    }

    @ApplicationScoped
    public static class MyBatchConsumingApp {

        AtomicInteger count = new AtomicInteger();

        @Incoming("kafka")
        public void consume(List<String> s, KafkaMessageObservation metadata) {
            assertThat(metadata).isNotNull();
            count.addAndGet(s.size());
        }

        public int count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class MyProducerApp {

        AtomicInteger acked = new AtomicInteger();

        @Outgoing("kafka")
        public Multi<Message<String>> produce() {
            return Multi.createFrom().items("1", "2", "3", "4", "5")
                    .map(s -> Message.of(s, () -> {
                        acked.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    }));
        }

        public int count() {
            return acked.get();
        }
    }

    @ApplicationScoped
    public static class MyEmitterProducerApp {
        AtomicInteger acked = new AtomicInteger();

        @Inject
        @Channel("kafka")
        Emitter<String> emitter;

        public void produce() {
            for (int i = 0; i < 5; i++) {
                emitter.send(Message.of(String.valueOf(i + 1), () -> {
                    acked.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }));
            }
        }

        public int count() {
            return acked.get();
        }
    }

    @ApplicationScoped
    public static class MyReactiveMessagingMessageObservationCollector
            implements MessageObservationCollector<ObservationContext> {

        private final List<MessageObservation> observations = new CopyOnWriteArrayList<>();

        @Override
        public MessageObservation onNewMessage(String channel, Message<?> message, ObservationContext ctx) {
            KafkaMessageObservation observation = new KafkaMessageObservation(channel, message);
            observations.add(observation);
            return observation;
        }

        public List<MessageObservation> getObservations() {
            return observations;
        }

        public static class KafkaMessageObservation extends DefaultMessageObservation {
            final long recordTs;
            final long createdMs;
            volatile long completedMs;

            public KafkaMessageObservation(String channel, Message<?> message) {
                super(channel);
                this.createdMs = System.currentTimeMillis();
                Optional<IncomingKafkaRecordMetadata> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
                if (metadata.isPresent()) {
                    Instant inst = metadata.get().getTimestamp();
                    recordTs = inst.toEpochMilli();
                } else {
                    recordTs = 0L;
                }
            }

            @Override
            public void onMessageAck(Message<?> message) {
                super.onMessageAck(message);
                completedMs = System.currentTimeMillis();
                done = true;
            }
        }
    }

}
