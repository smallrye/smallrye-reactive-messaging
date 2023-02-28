package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.record;
import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class ProducerTest extends KafkaCompanionTestBase {

    @Test
    void testProduceFromGenerator() {
        // Produce 100 records from generator
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, "key-" + i, i), 100);

        // Consume 100 and assert the count
        assertThat(companion.consume(Integer.class).withGroupId("group")
                .fromTopics(topic, 100).awaitCompletion().count()).isEqualTo(100);
    }

    @Test
    void testProduceWithSerializers() {
        companion.produceWithSerializers(IntegerSerializer.class, StringSerializer.class)
                .usingGenerator(i -> new ProducerRecord<>(topic, 1, Integer.toString(i)), 10);

        assertThat(companion.consume(Integer.class, String.class).fromTopics(topic, 10)
                .awaitCompletion().getRecords())
                .hasSize(10)
                .allSatisfy(c -> assertThat(c.key()).isEqualTo(1))
                .extracting(ConsumerRecord::value).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        companion.produceWithSerializers(new IntegerSerializer(), new StringSerializer())
                .usingGenerator(i -> new ProducerRecord<>(topic, 2, Integer.toString(i)), 10);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp(topic, 0), 10L);
        assertThat(companion.consume(Integer.class, String.class).fromOffsets(offsets, 10)
                .awaitCompletion().getRecords())
                .hasSize(10)
                .allSatisfy(c -> assertThat(c.key()).isEqualTo(2))
                .extracting(ConsumerRecord::value).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testProduceCsv() {
        // Produce records from csv file and capture number of records produced
        long producedItemCount = companion.produce(String.class)
                .fromCsv("produce.csv").awaitCompletion().count();

        // Consume from "messages" topic
        ConsumerTask<String, String> records = companion.consumeStrings()
                .fromTopics("messages", producedItemCount);

        // Await and count number of records consumed and assert it to produced number of items
        assertThat(records.awaitCompletion().count()).isEqualTo(producedItemCount);
    }

    @Test
    void testProduceFromList() {
        // Create topic and wait
        companion.topics().createAndWait(topic, 3);

        // Produce 3 records
        ProducerTask producerTask = companion.produceDoubles().fromRecords(Arrays.asList(
                record(topic, 0, "1", 0.1),
                record(topic, 1, "2", 0.2),
                record(topic, 2, "3", 0.3))).awaitCompletion();
        Map<TopicPartition, List<RecordMetadata>> records = producerTask.byTopicPartition();
        assertThat(records).containsOnlyKeys(tp(topic, 0), tp(topic, 1), tp(topic, 2));

        // Consume 3 items and assert values
        assertThat(companion.consumeDoubles().fromTopics(topic).awaitRecords(3).getRecords())
                .extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder(0.1, 0.2, 0.3);
    }

    @Test
    void testProducerWithProps() {
        Map<String, String> props = new HashMap<>();
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "5");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        ProducerTask records = companion.produceIntegers()
                .withProps(props)
                .withProp(ProducerConfig.ACKS_CONFIG, "1")
                .usingGenerator(i -> record(topic, i), 10);

        assertThat(records.awaitCompletion().count()).isEqualTo(10);
    }

    @Test
    void testProducerReuse() {
        // Create int producer
        String producerId = UUID.randomUUID().toString();
        ProducerBuilder<String, Integer> producer = companion.produceIntegers().withClientId(producerId);

        // Produce 1000 records by generating
        ProducerTask records = producer.usingGenerator(i -> new ProducerRecord<>(topic, i), 100);

        // Await production and check latest offset
        assertThat(records.awaitCompletion().lastOffset()).isEqualTo(99);

        // Consume 1000 check count
        assertThat(companion.consumeIntegers().fromTopics(topic, 100)
                .awaitCompletion().count()).isEqualTo(100);

        // Produce 1 more record and wait for ack
        producer.fromRecords(new ProducerRecord<>(topic, "key", 1)).awaitCompletion();

        // Produce 1000 more records and await completion
        producer.usingGenerator(i -> new ProducerRecord<>(topic, 100 + i), 100)
                .awaitCompletion();

        // Consume during at most 4 seconds and await
        ConsumerTask<String, Integer> records2 = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST) // this is the default
                .fromTopics(topic, Duration.ofSeconds(4))
                .awaitCompletion();

        // Check latest
        assertThat(records2.count()).isEqualTo(201);
        assertThat(records2.getLastRecord().offset()).isEqualTo(200);
    }

    @Test
    void testTransactional() throws InterruptedException {
        // Start producing 5000 records in a transaction
        ProducerTask produced = companion.produceStrings().withTransactionalId("tx")
                .usingGenerator(i -> record(topic, "key" + i, "v" + i), 1000);

        // Start consuming with read committed
        ConsumerTask<String, String> records = companion.consumeStrings()
                .withIsolationLevel(IsolationLevel.READ_COMMITTED)
                .fromTopics(topic, 1000);

        // Wait and check that no record is consumed
        // TODO ogu find a better way for testing this
        // Thread.sleep(1000);
        // assertThat(records.count()).isEqualTo(0);

        // Wait until producer is finished and thus committed transaction
        produced.awaitCompletion(Duration.ofMinutes(1));

        // Check that consumer received 5000 records
        assertThat(records.awaitCompletion().count()).isEqualTo(1000);
    }

    @Test
    void testProducerStop() {
        // Produce records without limit
        ProducerTask produced = companion.produceIntegers()
                .usingGenerator(i -> record(topic, "key", i));

        await().atMost(1, TimeUnit.MINUTES).until(() -> produced.count() >= 1000);
        produced.stop();

        long finalProducedCount = produced.awaitCompletion((throwable, cancelled) -> assertThat(cancelled).isTrue())
                .count();

        // Consume records until produced record
        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .fromTopics(topic, finalProducedCount);

        assertThat(records.awaitCompletion().count()).isEqualTo(finalProducedCount);
    }

}
