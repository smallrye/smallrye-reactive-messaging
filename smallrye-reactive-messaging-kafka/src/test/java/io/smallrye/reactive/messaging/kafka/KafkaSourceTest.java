package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.UnsatisfiedResolutionException;
import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testng.Assert;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.api.KafkaMetadataUtil;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaBrokerExtension;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.strimzi.test.container.StrimziKafkaContainer;

public class KafkaSourceTest extends KafkaCompanionTestBase {

    KafkaSource<String, Integer> source;
    KafkaConnector connector;

    @AfterEach
    public void closing() {
        if (source != null) {
            source.closeQuietly();
        }
        if (connector != null) {
            connector.terminate(new Object());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSource() {
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSourceWithPartitions() {
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("partitions", 4);

        companion.topics().createAndWait(topic, 3, Duration.ofMinutes(1));
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 1000);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1000);

        List<Integer> expected = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        // Because of partitions we cannot enforce the order.
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactlyInAnyOrderElementsOf(expected);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testSourceWithChannelName() {
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<KafkaRecord> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4,
                        5, 6, 7, 8, 9);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testBroadcast() {
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("broadcast", true);

        CountKafkaCdiEvents testEvents = new CountKafkaCdiEvents();

        connector = new KafkaConnector();
        connector.executionHolder = new ExecutionHolder(vertx);
        connector.configurations = UnsatisfiedInstance.instance();
        connector.consumerRebalanceListeners = UnsatisfiedInstance.instance();
        connector.kafkaCDIEvents = testEvents;
        connector.init();

        PublisherBuilder<? extends KafkaRecord> builder = (PublisherBuilder<? extends KafkaRecord>) connector
                .getPublisherBuilder(config);

        List<KafkaRecord> messages1 = new ArrayList<>();
        List<KafkaRecord> messages2 = new ArrayList<>();
        builder.forEach(messages1::add).run();
        builder.forEach(messages2::add).run();

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4,
                        5, 6, 7, 8, 9);
        assertThat(messages2.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(testEvents.firedConsumerEvents.sum()).isEqualTo(1);
        assertThat(testEvents.firedProducerEvents.sum()).isEqualTo(0);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testBroadcastWithPartitions() {
        companion.topics().createAndWait(topic, 2, Duration.ofMinutes(1));
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("broadcast", true)
                .with("partitions", 2);

        connector = new KafkaConnector();
        connector.executionHolder = new ExecutionHolder(vertx);
        connector.configurations = UnsatisfiedInstance.instance();
        connector.consumerRebalanceListeners = UnsatisfiedInstance.instance();
        connector.kafkaCDIEvents = new CountKafkaCdiEvents();
        connector.init();

        PublisherBuilder<? extends KafkaRecord> builder = (PublisherBuilder<? extends KafkaRecord>) connector
                .getPublisherBuilder(config);

        List<KafkaRecord> messages1 = new ArrayList<>();
        List<KafkaRecord> messages2 = new ArrayList<>();
        builder.forEach(messages1::add).run();
        builder.forEach(messages2::add).run();

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(0, 1, 2, 3, 4,
                        5, 6, 7, 8, 9);
        assertThat(messages2.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @SuppressWarnings({ "rawtypes" })
    @Test
    @Tag(TestTags.SLOW)
    public void testRetry() {
        // This test need an individual Kafka container
        try (StrimziKafkaContainer kafka = KafkaBrokerExtension.createKafkaContainer()) {
            kafka.start();
            await().until(kafka::isRunning);
            MapBasedConfig config = newCommonConfigForSource()
                    .with("bootstrap.servers", kafka.getBootstrapServers())
                    .with("value.deserializer", IntegerDeserializer.class.getName())
                    .with("retry", true)
                    .with("retry-attempts", 100)
                    .with("retry-max-wait", 30);

            KafkaCompanion kafkaCompanion = new KafkaCompanion(kafka.getBootstrapServers());

            KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
            source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                    UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents,
                    UnsatisfiedInstance.instance(), -1);
            List<KafkaRecord> messages1 = new ArrayList<>();
            source.getStream().subscribe().with(messages1::add);

            AtomicInteger counter = new AtomicInteger();
            kafkaCompanion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

            await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

            try (@SuppressWarnings("unused")
            StrimziKafkaContainer container = KafkaBrokerExtension.restart(kafka, 2)) {
                kafkaCompanion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

                await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
                assertThat(messages1.size()).isGreaterThanOrEqualTo(20);
            }
        }
    }

    //    @SuppressWarnings({ "rawtypes" })
    //    @Test
    //    public void testRecoveryAfterMissedHeartbeat() throws InterruptedException {
    //        MapBasedConfig config = newCommonConfigForSource()
    //                .with("bootstrap.servers", KafkaBrokerExtension.usage.getBootstrapServers())
    //                .with("value.deserializer", IntegerDeserializer.class.getName())
    //                .with(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000)
    //                .with(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100)
    //                .with("retry", true)
    //                .with("retry-attempts", 100)
    //                .with("retry-max-wait", 30);
    //
    //        usage.setBootstrapServers(KafkaBrokerExtension.usage.getBootstrapServers());
    //
    //        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
    //        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
    //                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents,
    //                UnsatisfiedInstance.instance(), -1);
    //        List<KafkaRecord> messages1 = new ArrayList<>();
    //        source.getStream().subscribe().with(messages1::add);
    //
    //        AtomicInteger counter = new AtomicInteger();
    //        usage.produceIntegers(10, null,
    //                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();
    //
    //        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
    //
    //        KafkaBrokerExtension.getProxy().setConnectionCut(true);
    //        Thread.sleep(6000 + 500); // session timeout + a bit more just in case.
    //        KafkaBrokerExtension.getProxy().setConnectionCut(false);
    //
    //        usage.produceIntegers(10, null,
    //                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();
    //
    //        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
    //        assertThat(messages1.size()).isGreaterThanOrEqualTo(20);
    //    }

    private KafkaMapBasedConfig myKafkaSourceConfig(String topic, int partitions, String withConsumerRebalanceListener,
            String group) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data");
        if (group != null) {
            config.put("group.id", group);
        }
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("topic", topic);
        if (partitions > 0) {
            config.put("partitions", Integer.toString(partitions));
        }
        if (withConsumerRebalanceListener != null) {
            config.put("consumer-rebalance-listener.name", withConsumerRebalanceListener);
        }

        return config;
    }

    private KafkaMapBasedConfig myKafkaSourceConfig(String topic,
            String group) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data");
        config.put("group.id", group);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("topic", topic);
        return config;
    }

    private KafkaMapBasedConfig myKafkaSourceConfigWithoutAck(String suffix, boolean shorterTimeouts) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data");
        config.put("group.id", "my-group-starting-on-fifth-" + suffix);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("topic", "data-starting-on-fifth-" + suffix);
        if (shorterTimeouts) {
            config.put("max.poll.interval.ms", "2000");
        }

        return config;
    }

    @Test
    public void testABeanConsumingTheKafkaMessages() {
        String topic = UUID.randomUUID().toString();
        String group = UUID.randomUUID().toString();
        ConsumptionBean bean = run(myKafkaSourceConfig(topic, group));
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<KafkaRecord<String, Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            assertThat(m.getTopic()).isEqualTo(topic);
            assertThat(m.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(m.getPartition()).isGreaterThan(-1);
        });

        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("data");
        assertThat(readiness.getChannels().get(0).getChannel()).isEqualTo("data");

        KafkaClientService service = get(KafkaClientService.class);
        assertThat(service.getConsumer("data")).isNotNull();
        assertThat(service.getConsumer("missing")).isNull();
        assertThatThrownBy(() -> service.getConsumer(null)).isInstanceOf(NullPointerException.class);
        assertThat(service.getConsumerChannels()).containsExactly("data");
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesMultiThread() {
        String group = UUID.randomUUID().toString();
        MultiThreadConsumer bean = runApplication(myKafkaSourceConfig(topic, 0, null, group),
                MultiThreadConsumer.class);
        List<Integer> list = bean.getItems();
        assertThat(list).isEmpty();
        bean.run();
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 100);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 100);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesWithPartitions() {
        companion.topics().createAndWait(topic, 2, Duration.ofMinutes(1));
        // Verify the creation of the topic
        assertThat(companion.topics().describe(topic).get(topic).partitions()).hasSize(2)
                .allSatisfy(tpi -> assertThat(tpi.leader()).isNotNull()
                        .satisfies(node -> assertThat(node.id()).isGreaterThanOrEqualTo(0)));

        ConsumptionBean bean = run(
                myKafkaSourceConfig(topic, 2, ConsumptionConsumerRebalanceListener.class.getSimpleName(), null));
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsOnly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<KafkaRecord<String, Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            assertThat(m.getTopic()).isEqualTo(topic);
            assertThat(m.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(m.getPartition()).isGreaterThan(-1);
        });

        ConsumptionConsumerRebalanceListener consumptionConsumerRebalanceListener = getConsumptionConsumerRebalanceListener();
        assertThat(consumptionConsumerRebalanceListener.getAssigned().size()).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            TopicPartition partition = consumptionConsumerRebalanceListener.getAssigned().get(i);
            assertThat(partition).isNotNull();
            assertThat(partition.topic()).isEqualTo(topic);
        }
    }

    @Test
    public void testABeanConsumingWithMissingRebalanceListenerConfiguredByName() {
        String group = UUID.randomUUID().toString();
        assertThatThrownBy(() -> run(myKafkaSourceConfig(topic, 0, "not exists", group)))
                .isInstanceOf(DeploymentException.class)
                .hasCauseInstanceOf(UnsatisfiedResolutionException.class);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesStartingOnFifthOffsetFromLatest() {
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>("data-starting-on-fifth-happy-path", i), 10)
                .awaitCompletion(Duration.ofMinutes(2));
        /*
         * Will use StartFromFifthOffsetFromLatestConsumerRebalanceListener
         */
        ConsumptionBeanWithoutAck bean = runWithoutAck(
                myKafkaSourceConfigWithoutAck("happy-path", false));
        List<Integer> list = bean.getResults();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> list.size() >= 5);

        assertThat(list).containsExactly(6, 7, 8, 9, 10);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesStartingOnFifthOffsetFromLatestThatFailsOnTheFirstAttempt() {
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>("data-starting-on-fifth-fail-on-first-attempt", i), 10)
                .awaitCompletion(Duration.ofMinutes(2));
        /*
         * Will use StartFromFifthOffsetFromLatestConsumerRebalanceListener
         */
        ConsumptionBeanWithoutAck bean = runWithoutAck(
                myKafkaSourceConfigWithoutAck("fail-on-first-attempt", false));
        List<Integer> list = bean.getResults();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> list.size() >= 5);

        assertThat(list).containsExactly(6, 7, 8, 9, 10);

        assertThat(
                getStartFromFifthOffsetFromLatestConsumerRebalanceListener(
                        "my-group-starting-on-fifth-fail-on-first-attempt")
                                .getRebalanceCount()).isEqualTo(1);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesStartingOnFifthOffsetFromLatestThatFailsUntilSecondRebalance() {
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>("data-starting-on-fifth-fail-until-second-rebalance", i), 10)
                .awaitCompletion(Duration.ofMinutes(2));
        /*
         * Will use StartFromFifthOffsetFromLatestConsumerRebalanceListener
         */
        ConsumptionBeanWithoutAck bean = runWithoutAck(
                myKafkaSourceConfigWithoutAck("fail-until-second-rebalance", false));
        List<Integer> list = bean.getResults();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> list.size() >= 10);

        // The rebalance listener failed, no retry
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Failed, not called and there is no retry
        assertThat(getStartFromFifthOffsetFromLatestConsumerRebalanceListener(
                "my-group-starting-on-fifth-fail-until-second-rebalance").getRebalanceCount()).isEqualTo(0);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInvalidIncomingType() {
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 2);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 2);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1);

        companion.produceStrings().fromRecords(new ProducerRecord<>(topic, "hello"));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 2);

        // no other message received
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testABeanConsumingTheKafkaMessagesWithRawMessage() {
        String group = UUID.randomUUID().toString();
        ConsumptionBeanUsingRawMessage bean = runApplication(myKafkaSourceConfig(topic, 0, null, group),
                ConsumptionBeanUsingRawMessage.class);
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<Message<Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            // TODO Import normally once the deprecateed copy in this package has gone
            io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata<String, Integer> metadata = m
                    .getMetadata(io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata.class)
                    .orElseThrow(() -> new AssertionError("Metadata expected"));
            assertThat(metadata.getTopic()).isEqualTo(topic);
            assertThat(metadata.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(metadata.getPartition()).isGreaterThan(-1);
            assertThat(metadata.getOffset()).isGreaterThan(-1);
            Assert.assertSame(metadata, KafkaMetadataUtil.readIncomingKafkaMetadata(m).get());
            LegacyMetadataTestUtils.tempCompareLegacyAndApiMetadata(metadata, m);
        });

        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("data");
        assertThat(readiness.getChannels().get(0).getChannel()).isEqualTo("data");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSourceWithEmptyOptionalConfiguration() {
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("sasl.jaas.config", "") //optional configuration
                .with("sasl.mechanism", ""); //optional configuration
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private ConsumptionConsumerRebalanceListener getConsumptionConsumerRebalanceListener() {
        return getBeanManager()
                .createInstance()
                .select(ConsumptionConsumerRebalanceListener.class)
                .select(Identifier.Literal.of(ConsumptionConsumerRebalanceListener.class.getSimpleName()))
                .get();

    }

    private StartFromFifthOffsetFromLatestConsumerRebalanceListener getStartFromFifthOffsetFromLatestConsumerRebalanceListener(
            String name) {
        return getBeanManager()
                .createInstance()
                .select(StartFromFifthOffsetFromLatestConsumerRebalanceListener.class)
                .select(Identifier.Literal.of(name))
                .get();

    }

    private ConsumptionBean run(KafkaMapBasedConfig config) {
        addBeans(ConsumptionBean.class, ConsumptionConsumerRebalanceListener.class);
        runApplication(config);
        return get(ConsumptionBean.class);
    }

    private ConsumptionBeanWithoutAck runWithoutAck(KafkaMapBasedConfig config) {
        addBeans(ConsumptionBeanWithoutAck.class, ConsumptionConsumerRebalanceListener.class,
                StartFromFifthOffsetFromLatestConsumerRebalanceListener.class,
                StartFromFifthOffsetFromLatestButFailOnFirstConsumerRebalanceListener.class,
                StartFromFifthOffsetFromLatestButFailUntilSecondRebalanceConsumerRebalanceListener.class);
        runApplication(config);
        return get(ConsumptionBeanWithoutAck.class);
    }

    @ApplicationScoped
    public static class MultiThreadConsumer {

        private final List<Integer> items = new CopyOnWriteArrayList<>();
        private final List<String> threads = new CopyOnWriteArrayList<>();
        private final Random random = new Random();
        ExecutorService executor = Executors.newFixedThreadPool(10);

        @Inject
        @Channel("data")
        Multi<Integer> messages;

        public void run() {
            messages
                    .emitOn(executor)
                    .onItem().transform(s -> {
                        try {
                            Thread.sleep(random.nextInt(100));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return s * -1;
                    })
                    .subscribe().with(it -> {
                        items.add(it);
                        threads.add(Thread.currentThread().getName());
                    });
        }

        public List<Integer> getItems() {
            return items;
        }

        public List<String> getThreads() {
            return threads;
        }
    }

}
