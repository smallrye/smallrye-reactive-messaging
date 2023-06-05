package io.smallrye.reactive.messaging.kafka.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.observation.ObservationMetadata;
import io.smallrye.reactive.messaging.observation.ReactiveMessagingObservation;
import io.smallrye.reactive.messaging.providers.extension.ObservationCenter;

public class ObservationTest extends KafkaCompanionTestBase {

    @Test
    void testWithIndividualMessages() {
        addBeans(MyReactiveMessagingObservation.class);
        addBeans(MyApp.class);

        runApplication(kafkaConfig("mp.messaging.incoming.kafka", false)
                .with("topic", topic)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));

        companion.produceStrings()
                .fromMulti(Multi.createFrom().range(0, 5).map(i -> new ProducerRecord<>(topic, null, Integer.toString(i))));

        ObservationCenter oc = get(ObservationCenter.class);
        await().untilAsserted(() -> {
            MyReactiveMessagingObservation observation = (MyReactiveMessagingObservation) oc.getObservation();
            assertThat(observation.getObservations()).hasSize(5);
            assertThat(observation.getObservations()).allSatisfy(o -> {
                assertThat(o.getCreationTime()).isNotEqualTo(-1);
                assertThat(o.getProcessingTime()).isNotEqualTo(-1);
                assertThat(o.getCompletionTime()).isNotEqualTo(-1);
                assertThat(o.getReason()).isNull();
            });
        });
    }

    @Test
    void testWithBatchesMessages() {
        addBeans(MyReactiveMessagingObservation.class);
        addBeans(MyAppUsingBatches.class);

        runApplication(kafkaConfig("mp.messaging.incoming.kafka", false)
                .with("topic", topic)
                .with("batch", true)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));

        companion.produceStrings()
                .fromMulti(Multi.createFrom().range(0, 1000).map(i -> new ProducerRecord<>(topic, null, Integer.toString(i))));

        ObservationCenter oc = get(ObservationCenter.class);
        MyAppUsingBatches batches = get(MyAppUsingBatches.class);
        await().untilAsserted(() -> {
            MyReactiveMessagingObservation observation = (MyReactiveMessagingObservation) oc.getObservation();
            assertThat(batches.count()).isEqualTo(1000);
            assertThat(observation.getObservations()).allSatisfy(o -> {
                assertThat(o.getCreationTime()).isNotEqualTo(-1);
                assertThat(o.getProcessingTime()).isNotEqualTo(-1);
                assertThat(o.getCompletionTime()).isNotEqualTo(-1);
                assertThat(o.getReason()).isNull();
            });
        });
    }

    @ApplicationScoped
    public static class MyApp {
        @Incoming("kafka")
        public void consume(String ignored, ObservationMetadata metadata) {
            assertThat(metadata).isNotNull();
        }
    }

    @ApplicationScoped
    public static class MyAppUsingBatches {

        AtomicInteger count = new AtomicInteger();

        @Incoming("kafka")
        public void consume(List<String> s, ObservationMetadata metadata) {
            assertThat(metadata).isNotNull();
            count.addAndGet(s.size());
        }

        public int count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class MyReactiveMessagingObservation implements ReactiveMessagingObservation {

        private final List<MyMessageObservation> observations = new CopyOnWriteArrayList<>();

        @Override
        public MessageObservation onNewMessage(String channel, Message<?> message) {
            MyMessageObservation observation = new MyMessageObservation(channel, message);
            observations.add(observation);
            return observation;
        }

        public List<MyMessageObservation> getObservations() {
            return observations;
        }

        public static class MyMessageObservation extends MessageObservation {

            public MyMessageObservation(String channel, Message<?> message) {
                super(channel, message);
            }

            public long getCreationTime() {
                return creationTime;
            }

            public long getProcessingTime() {
                return processing;
            }

            public long getCompletionTime() {
                return completion;
            }

            public Throwable getReason() {
                return nackReason;
            }
        }
    }

}
