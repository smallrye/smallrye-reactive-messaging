package io.smallrye.reactive.messaging.kafka.companion;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SerdesTest extends KafkaCompanionTestBase {

    @Test
    void testRegisteredSerdeShouldBeConfigured() {
        // Custom Serde which Serialize/Deserializer throw exception if not configured
        companion.registerSerde(Person.class, new Serde<>() {
            private PersonDeserializer personDeserializer = new PersonDeserializer();
            private PersonSerializer personSerializer = new PersonSerializer();

            @Override
            public Serializer<Person> serializer() {
                return personSerializer;
            }

            @Override
            public Deserializer<Person> deserializer() {
                return personDeserializer;
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // Configure the serializer and deserializer
                personSerializer.configure(configs, isKey);
                personDeserializer.configure(configs, isKey);
            }
        });

        companion.produce(Person.class).fromRecords(
                new ProducerRecord<>(topic, new Person("1", 30)),
                new ProducerRecord<>(topic, new Person("2", 25)),
                new ProducerRecord<>(topic, new Person("3", 18))).awaitCompletion();

        ConsumerBuilder<String, Person> consumer = companion.consumeWithDeserializers(PersonDeserializer.class.getName());
        ConsumerTask<String, Person> task = consumer.fromTopics(topic, 3).awaitCompletion();
        assertThat(task.getRecords()).hasSize(3);
    }

    @Test
    void testProduceRegisteredSerdeConsumeWithSerdeName() {
        companion.registerSerde(Person.class, new PersonSerializer(), new PersonDeserializer());

        companion.produce(Person.class).fromRecords(
                new ProducerRecord<>(topic, new Person("1", 30)),
                new ProducerRecord<>(topic, new Person("2", 25)),
                new ProducerRecord<>(topic, new Person("3", 18))).awaitCompletion();

        ConsumerBuilder<String, Person> consumer = companion.consumeWithDeserializers(PersonDeserializer.class.getName());
        ConsumerTask<String, Person> task = consumer.fromTopics(topic, 3).awaitCompletion();
        assertThat(task.getRecords()).hasSize(3);
    }

    @Test
    void testProduceWithSerdeNameConsumeWithRegisteredSerde() {
        companion.registerSerde(Person.class, new PersonSerializer(), new PersonDeserializer());

        companion.produceWithSerializers(PersonSerializer.class.getName())
                .fromRecords(
                        new ProducerRecord<>(topic, new Person("1", 30)),
                        new ProducerRecord<>(topic, new Person("2", 25)),
                        new ProducerRecord<>(topic, new Person("3", 18)))
                .awaitCompletion();

        ConsumerBuilder<String, Person> consumer = companion.consume(Person.class);
        ConsumerTask<String, Person> task = consumer.fromTopics(topic, 3).awaitCompletion();
        assertThat(task.getRecords()).hasSize(3);
    }

    @Test
    void testProduceWithSerdeNameConsumeWithGenericSerde() {
        companion.registerSerde(Person.class, new GenericSerializer<>(), new GenericDeserializer<>());

        ProducerRecord<String, Person> record = new ProducerRecord<>(topic, new Person("1", 20));
        companion.produce(Person.class)
                .fromRecords(record)
                .awaitCompletion();

        ConsumerBuilder<String, Person> consumer = companion.consume(Person.class);
        ConsumerTask<String, Person> task = consumer.fromTopics(topic, 1).awaitCompletion();
        assertThat(task.getRecords()).hasSize(1);
    }

    @Test
    void testProduceWithSerdeNameConsumeWithGenericSerdeWithSerdeName() {
        ProducerRecord<String, Person> record = new ProducerRecord<>(topic, new Person("1", 20));
        companion.<String, Person> produceWithSerializers(GenericSerializer.class.getName())
                .fromRecords(record)
                .awaitCompletion();

        ConsumerBuilder<String, Person> consumer = companion.consumeWithDeserializers(GenericDeserializer.class.getName());
        ConsumerTask<String, Person> task = consumer.fromTopics(topic, 1).awaitCompletion();
        assertThat(task.getRecords()).hasSize(1);
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

        private boolean isConfigured = false;

        @Override
        public byte[] serialize(String s, Person person) {
            if (isConfigured) {
                return (person.id + "|" + person.age).getBytes();
            } else {
                throw new SerializationException("Serializer not configured");
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            isConfigured = true;
        }
    }

    public static class GenericSerializer<T> implements Serializer<T> {

        @Override
        public byte[] serialize(String s, T object) {
            return object.toString().getBytes();
        }
    }

    public static class GenericDeserializer<T> implements Deserializer<T> {
        @Override
        public T deserialize(String s, byte[] bytes) {
            return null;
        }
    }

    public static class PersonDeserializer implements Deserializer<Person> {
        private boolean isConfigured = false;

        @Override
        public Person deserialize(String s, byte[] bytes) {
            if (isConfigured) {
                String[] split = new String(bytes).split("\\|");
                return new Person(split[0], Integer.parseInt(split[1]));
            } else {
                throw new SerializationException("Deserializer not configured");
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            isConfigured = true;
        }
    }
}
