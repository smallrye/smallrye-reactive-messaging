package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.record;
import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class StreamTest extends KafkaCompanionTestBase {

    @Test
    void testBroadcast() {
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 500);

        ConsumerBuilder<String, String> consumer = companion.consumeStrings();
        ConsumerTask<String, String> records = consumer.fromTopics(topic);

        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        records.getMulti().subscribe().with(list::add);

        assertThat(records.awaitRecords(500).count()).isGreaterThanOrEqualTo(500);
        await().until(() -> list.size() == 500);
        assertThat(records.getRecords()).containsExactlyElementsOf(list);
    }

    @Test
    void testProduceConsumeProduce() {
        String newTopic = topic + "-new";
        String newTopic2 = topic + "-new2";
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 500);

        ConsumerTask<String, String> consumer = companion.consumeStrings().fromTopics(topic);

        Multi<ProducerRecord<String, Integer>> processor = consumer.getMulti()
                .onItem().transform(cr -> record(newTopic, cr.key(), Integer.parseInt(cr.value().substring(1))));
        ProducerTask records = companion.produceIntegers().fromMulti(processor);

        Multi<ProducerRecord<String, String>> processor2 = consumer.getMulti()
                .onItem().transform(cr -> record(newTopic2, cr.key(), "v" + cr.value().substring(1)));
        ProducerTask records2 = companion.produceStrings().fromMulti(processor2);

        assertThat(consumer.awaitRecords(500).count()).isEqualTo(500);
        assertThat(records.awaitRecords(500).count()).isEqualTo(500);
        assertThat(records2.awaitRecords(500).count()).isEqualTo(500);
    }

    @Test
    void testProcessAndAwaitRecords() {
        try (ProducerTask source = companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, "t" + i));
                ProducerTask process = companion.process(Collections.singleton(topic),
                        companion.consumeStrings().withClientId("process-consumer"),
                        companion.produceStrings().withClientId("process-producer"),
                        r -> new ProducerRecord<>(topic + "-new", r.partition(), r.key(), r.value()))) {
            process.awaitRecords(10);
            process.awaitNextRecord();
            process.awaitNextRecords(1);
            process.awaitNextRecords(1, Duration.ofSeconds(1));

            assertThat(process.getFirstRecord().offset()).isEqualTo(0L);
        }
    }

    @Test
    void testProcessTransaction() {
        String newTopic = topic + "-new";
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 1000)
                .awaitCompletion();

        ProducerTask txProcess = companion.processTransactional(Collections.singleton(topic),
                companion.consumeStrings(), companion.produceIntegers().withTransactionalId("tx-producer"),
                record -> new ProducerRecord<>(newTopic, record.partition(), record.key(),
                        Integer.parseInt(record.value().substring(1))));

        txProcess.awaitRecords(1000).stop();
    }

    @Test
    void testProcessTransactionWithError() {
        String newTopic = topic + "-new";
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 1000)
                .awaitCompletion();

        ProducerTask txProcess = companion.processTransactional(Collections.singleton(topic),
                companion.consumeStrings().withGroupId("tx-consumer"),
                companion.produceIntegers().withTransactionalId("tx-producer").withConcurrency(1),
                record -> {
                    if (record.value().equals("t600")) {
                        throw new IllegalArgumentException("Cannot process" + record);
                    }
                    return new ProducerRecord<>(newTopic, record.partition(), record.key(),
                            Integer.parseInt(record.value().substring(1)));
                });

        txProcess.awaitCompletion((throwable, aBoolean) -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class));
        assertThat(txProcess.count()).isEqualTo(600L);
        OffsetAndMetadata offsets = companion.consumerGroups().offsets("tx-consumer", tp(topic, 0));
        assertThat(offsets.offset()).isEqualTo(500L);
    }
}
