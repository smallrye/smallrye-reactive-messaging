package kafka.companion;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class ProducerTest extends KafkaCompanionTestBase {

    @Test
    public void fromRecords() {
        // tag::records[]
        companion.produce(byte[].class).fromRecords(
                new ProducerRecord<>("topic1", "k1", "1".getBytes()),
                new ProducerRecord<>("topic1", "k2", "2".getBytes()),
                new ProducerRecord<>("topic1", "k3", "3".getBytes()));
        // end::records[]
    }

    @Test
    public void fromGenerator() {
        // tag::generator[]
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>("topic", i), 10);
        // end::generator[]
    }

    @Test
    public void fromCsv() {
        // tag::csv[]
        companion.produceStrings().fromCsv("records.csv");
        // end::csv[]
    }

}
