package io.smallrye.reactive.messaging.kafka.companion;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class SerdesTest extends KafkaCompanionTestBase {

    @Test
    void testProduceRegisteredSerdeConsumeWithSerdeName() {
        companion.registerSerde(Person.class, new PersonSerializer(), new PersonDeserializer());

        companion.produce(Person.class).fromRecords(
                new ProducerRecord<>(topic, new Person("1", 30)),
                new ProducerRecord<>(topic, new Person("2", 25)),
                new ProducerRecord<>(topic, new Person("3", 18))).awaitCompletion();

        ConsumerBuilder<String, Person> consumer = companion.consumeWithDeserializers(StringDeserializer.class.getName(),
                PersonDeserializer.class.getName());
        ConsumerTask<String, Person> task = consumer.fromTopics(topic, 3).awaitCompletion();
        assertThat(task.getRecords()).hasSize(3);
    }

    @Test
    void testProduceWithSerdeNameConsumeWithRegisteredSerde() {
        companion.registerSerde(Person.class, new PersonSerializer(), new PersonDeserializer());

        companion.produceWithSerializers(StringSerializer.class.getName(), PersonSerializer.class.getName())
                .fromRecords(
                        new ProducerRecord<>(topic, new Person("1", 30)),
                        new ProducerRecord<>(topic, new Person("2", 25)),
                        new ProducerRecord<>(topic, new Person("3", 18)))
                .awaitCompletion();

        ConsumerBuilder<String, Person> consumer = companion.consume(Person.class);
        ConsumerTask<String, Person> task = consumer.fromTopics(topic, 3).awaitCompletion();
        assertThat(task.getRecords()).hasSize(3);
    }

    public static class Person {

        public String id;
        public int age;

        public Person(String id, int age) {
            this.id = id;
            this.age = age;
        }
    }

    public static class PersonSerializer implements Serializer<Person> {

        @Override
        public byte[] serialize(String s, Person person) {
            return (person.id + "|" + person.age).getBytes();
        }
    }

    public static class PersonDeserializer implements Deserializer<Person> {

        @Override
        public Person deserialize(String s, byte[] bytes) {
            String[] split = new String(bytes).split("\\|");
            return new Person(split[0], Integer.parseInt(split[1]));
        }
    }
}
