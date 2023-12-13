package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import jakarta.enterprise.inject.Any;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;

public class AssignSeekOffsetsConsumerTest extends KafkaCompanionTestBase {

    @Test
    void testAssignWithoutSeek() {
        addBeans(ConsumptionConsumerRebalanceListener.class);
        companion.topics().createAndWait(topic, 3);
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i % 3, "k-" + i, i), 10)
                .awaitCompletion();
        ConsumptionBean app = runApplication(kafkaConfig("mp.messaging.incoming.data")
                .with("topic", topic)
                .with("auto.offset.reset", "earliest")
                .with("assign-seek", "0")
                .with("value.deserializer", IntegerDeserializer.class.getName()), ConsumptionBean.class);

        await().untilAsserted(() -> assertThat(app.getResults()).hasSize(4));
        assertThat(app.getKafkaMessages())
                .extracting(m -> m.getMetadata(IncomingKafkaRecordMetadata.class).get().getOffset())
                .containsExactly(0L, 1L, 2L, 3L);
        // rebalance listener is never called
        ConsumptionConsumerRebalanceListener listener = getBeanManager().createInstance()
                .select(ConsumptionConsumerRebalanceListener.class, Any.Literal.INSTANCE).get();
        await().untilAsserted(() -> assertThat(listener.getAssigned()).isEmpty());
    }

    @Test
    void testAssignSeek() {
        addBeans(ConsumptionConsumerRebalanceListener.class);
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "k-" + i, i), 20)
                .awaitCompletion();
        ConsumptionBean app = runApplication(kafkaConfig("mp.messaging.incoming.data")
                .with("topic", topic)
                .with("assign-seek", "0:10")
                .with("value.deserializer", IntegerDeserializer.class.getName()), ConsumptionBean.class);

        await().untilAsserted(() -> assertThat(app.getResults()).hasSize(10));
        assertThat(app.getKafkaMessages())
                .extracting(m -> m.getMetadata(IncomingKafkaRecordMetadata.class).get().getOffset())
                .containsExactly(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L);
        // rebalance listener is never called
        ConsumptionConsumerRebalanceListener listener = getBeanManager().createInstance()
                .select(ConsumptionConsumerRebalanceListener.class, Any.Literal.INSTANCE).get();
        await().untilAsserted(() -> assertThat(listener.getAssigned()).isEmpty());
    }

    @Test
    void testAssignSeekMultipleTopics() {
        addBeans(ConsumptionConsumerRebalanceListener.class);
        String topic1 = companion.topics().createAndWait(topic + "-1", 3);
        String topic2 = companion.topics().createAndWait(topic + "-2", 3);
        String topic3 = companion.topics().createAndWait(topic + "-3", 3);
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic1, i % 3, "k-" + i, i), 10)
                .awaitCompletion();
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic2, i % 3, "k-" + i, i), 10)
                .awaitCompletion();
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic3, i % 3, "k-" + i, i), 10)
                .awaitCompletion();
        ConsumptionBean app = runApplication(kafkaConfig("mp.messaging.incoming.data")
                .with("topic", topic)
                .with("assign-seek", String.format("%s:0:0, %s:1:-1, %s:2:2", topic1, topic2, topic3))
                .with("value.deserializer", IntegerDeserializer.class.getName()), ConsumptionBean.class);

        await().untilAsserted(() -> assertThat(app.getResults()).hasSize(5));
        assertThat(app.getKafkaMessages())
                .extracting(m -> m.getMetadata(IncomingKafkaRecordMetadata.class).get().getRecord())
                .extracting(ConsumerRecord::topic, ConsumerRecord::partition, ConsumerRecord::offset)
                .containsExactlyInAnyOrder(
                        Tuple.tuple(topic1, 0, 0L),
                        Tuple.tuple(topic1, 0, 1L),
                        Tuple.tuple(topic1, 0, 2L),
                        Tuple.tuple(topic1, 0, 3L),
                        Tuple.tuple(topic3, 2, 2L));

        // rebalance listener is never called
        ConsumptionConsumerRebalanceListener listener = getBeanManager().createInstance()
                .select(ConsumptionConsumerRebalanceListener.class, Any.Literal.INSTANCE).get();
        await().untilAsserted(() -> assertThat(listener.getAssigned()).isEmpty());
    }

}
