package kafka.companion;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Assertions;

import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;

public class KafkaTest {

    public void newCompanion() {
        // <companion>
        // Create companion with bootstrap servers and API timeout (default is 10 seconds)
        KafkaCompanion companion = new KafkaCompanion("localhost:9092", Duration.ofSeconds(5));

        // Create producer and start producer task
        ProducerBuilder<String, Integer> producer = companion.produceIntegers()
                .withClientId("my-producer")
                .withProp("max.block.ms", "5000");
        producer.usingGenerator(i -> new ProducerRecord<>("topic", i), 100);

        // Create consumer and start consumer task
        ConsumerBuilder<String, Integer> consumer = companion.consumeIntegers()
                .withGroupId("my-group")
                .withCommitAsyncWhen(record -> true);
        ConsumerTask<String, Integer> records = consumer.fromTopics("topic", Duration.ofSeconds(10));
        // Await completion and assert consumed record count
        Assertions.assertEquals(records.awaitCompletion().count(), 100);
        // </companion>
    }

    public void serdes() {
        // <serdes>
        KafkaCompanion companion = new KafkaCompanion("localhost:9092");

        // Register serde to the companion
        companion.registerSerde(Orders.class, new OrdersSerializer(), new OrdersDeserializer());

        // Companion will configure consumer accordingly
        ConsumerTask<Integer, Orders> orders = companion.consume(Integer.class, Orders.class)
                .fromTopics("orders", 1000).awaitCompletion();

        for (ConsumerRecord<Integer, Orders> order : orders) {
            // ... check consumed records
        }
        // </serdes>
    }

    public static class Orders {

    }

    public static class OrdersSerializer implements Serializer<Orders> {

        @Override
        public byte[] serialize(String s, Orders orders) {
            return new byte[0];
        }
    }

    public static class OrdersDeserializer implements Deserializer<Orders> {

        @Override
        public Orders deserialize(String s, byte[] bytes) {
            return null;
        }
    }

}
