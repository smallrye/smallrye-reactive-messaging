package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

class ChannelBindingKafkaTest extends KafkaCompanionTestBase {

    private Config incomingConfig(String channel) {
        return kafkaConfig("mp.messaging.incoming." + channel)
                .with("topic", topic)
                .with("group.id", UUID.randomUUID().toString())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("auto.offset.reset", "earliest")
                .with("enable.auto.commit", "false")
                .build();
    }

    private Config outgoingConfig(String channel) {
        return kafkaConfig("mp.messaging.outgoing." + channel)
                .with("topic", topic + "-out")
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", IntegerSerializer.class.getName())
                .build();
    }

    @Test
    void subscribeWithConsumer() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .subscribe((Consumer<Integer>) received::add);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(received).hasSize(5));
        cancellable.cancel();
    }

    @Test
    void subscribeWithMetadata() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, "key-" + i, i), 5);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Metadata> metadataList = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .subscribe((payload, metadata) -> {
                    received.add(payload);
                    metadataList.add(metadata);
                });

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    assertThat(received).hasSize(5);
                    assertThat(metadataList).hasSize(5);
                    assertThat(metadataList).allSatisfy(m -> assertThat(m.get(IncomingKafkaRecordMetadata.class)).isPresent());
                });
        cancellable.cancel();
    }

    @Test
    void subscribeAsync() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                    return Uni.createFrom().voidItem();
                });

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(received).hasSize(5));
        cancellable.cancel();
    }

    @Test
    void processAndSubscribe() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        List<String> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .process(i -> "val-" + i)
                .subscribe((Consumer<String>) received::add);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(received).hasSize(5));
        assertThat(received).allSatisfy(s -> assertThat(s).startsWith("val-"));
        cancellable.cancel();
    }

    @Test
    void processAndForwardToOutgoing() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .process(i -> i * 10)
                .to("test-out", outgoingConfig("test-out"));

        List<Integer> forwarded = new CopyOnWriteArrayList<>();
        companion.consumeIntegers().fromTopics(topic + "-out", 5)
                .awaitCompletion(Duration.ofSeconds(30))
                .getRecords()
                .forEach(r -> forwarded.add(r.value()));

        assertThat(forwarded).hasSize(5);
        assertThat(forwarded).allSatisfy(v -> assertThat(v % 10).isEqualTo(0));
    }

    @Test
    void blockingSubscribe() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .blocking()
                .subscribe(payload -> {
                    received.add(payload);
                    threadNames.add(Thread.currentThread().getName());
                });

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(received).hasSize(5));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).doesNotContain("vert.x"));
        cancellable.cancel();
    }

    @Test
    void cancelStopsConsumption() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .subscribe((Consumer<Integer>) received::add);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 3);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(received).hasSize(3));

        cancellable.cancel();

        int sizeAfterCancel = received.size();
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 100 + i), 3);

        // Wait a bit and verify no more messages arrive
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(received).hasSize(sizeAfterCancel);
    }

    @Test
    void subscribeNoArgAcks() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        List<Integer> seen = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .process(i -> {
                    seen.add(i);
                    return i;
                })
                .subscribe();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(seen).hasSize(5));
        cancellable.cancel();
    }

    @Test
    void processAsyncAndSubscribe() {
        runApplication(new MapBasedConfig());
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        List<String> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", incomingConfig("test-in"), Integer.class)
                .<String> processAsync(i -> Uni.createFrom().item("async-" + i))
                .subscribe((Consumer<String>) received::add);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(received).hasSize(5));
        assertThat(received).allSatisfy(s -> assertThat(s).startsWith("async-"));
        cancellable.cancel();
    }

    @Test
    void subscribeWithFlatConfig() {
        runApplication(kafkaConfig()
                .with("mp.messaging.connector.smallrye-kafka.bootstrap.servers", companion.getBootstrapServers())
                .with("mp.messaging.connector.smallrye-kafka.graceful-shutdown", false)
                .with("mp.messaging.connector.smallrye-kafka.tracing-enabled", false));
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", Map.of(
                "connector", KafkaConnector.CONNECTOR_NAME,
                "topic", topic,
                "group.id", UUID.randomUUID().toString(),
                "key.deserializer", StringDeserializer.class.getName(),
                "value.deserializer", IntegerDeserializer.class.getName(),
                "auto.offset.reset", "earliest",
                "enable.auto.commit", "false"), Integer.class)
                .subscribe((Consumer<Integer>) received::add);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(received).hasSize(5));
        cancellable.cancel();
    }

    @Test
    void subscribeWithFlatConfigInheritsConnectorDefaults() {
        runApplication(kafkaConfig()
                .with("mp.messaging.connector.smallrye-kafka.bootstrap.servers", companion.getBootstrapServers())
                .with("mp.messaging.connector.smallrye-kafka.graceful-shutdown", false)
                .with("mp.messaging.connector.smallrye-kafka.tracing-enabled", false)
                .with("mp.messaging.connector.smallrye-kafka.key.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.connector.smallrye-kafka.value.deserializer", IntegerDeserializer.class.getName())
                .with("mp.messaging.connector.smallrye-kafka.auto.offset.reset", "earliest")
                .with("mp.messaging.connector.smallrye-kafka.enable.auto.commit", "false"));
        ChannelFactory factory = get(ChannelFactory.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 5);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", Map.of(
                "connector", KafkaConnector.CONNECTOR_NAME,
                "topic", topic,
                "group.id", UUID.randomUUID().toString()), Integer.class)
                .subscribe((Consumer<Integer>) received::add);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(received).hasSize(5));
        cancellable.cancel();
    }
}
