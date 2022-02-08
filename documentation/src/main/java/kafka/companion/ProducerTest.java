package kafka.companion;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class ProducerTest extends KafkaCompanionTestBase {

    @Test
    public void fromRecords() {
        // <records>
        companion.produce(byte[].class).fromRecords(
                new ProducerRecord<>("topic1", "k1", "1".getBytes()),
                new ProducerRecord<>("topic1", "k2", "2".getBytes()),
                new ProducerRecord<>("topic1", "k3", "3".getBytes()));
        // </records>
    }

    @Test
    public void fromGenerator() {
        // <generator>
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>("topic", i), 10);
        // </generator>
    }

    @Test
    public void fromCsv() {
        // <csv>
        companion.produceStrings().fromCsv("records.csv");
        // </csv>
    }

}
