package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.UnsatisfiedResolutionException;
import javax.enterprise.inject.literal.NamedLiteral;
import javax.inject.Inject;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.strimzi.StrimziKafkaContainer;

public class KafkaSourceTest extends KafkaTestBase {

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

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, "hello", counter.getAndIncrement()))).start();

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

        createTopic(topic, 3);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(1000, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

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

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

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
        connector.defaultKafkaConfiguration = UnsatisfiedInstance.instance();
        connector.consumerRebalanceListeners = UnsatisfiedInstance.instance();
        connector.kafkaCDIEvents = testEvents;
        connector.init();

        PublisherBuilder<? extends KafkaRecord> builder = (PublisherBuilder<? extends KafkaRecord>) connector
                .getPublisherBuilder(config);

        List<KafkaRecord> messages1 = new ArrayList<>();
        List<KafkaRecord> messages2 = new ArrayList<>();
        builder.forEach(messages1::add).run();
        builder.forEach(messages2::add).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

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
        createTopic(topic, 2);
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("broadcast", true)
                .with("partitions", 2);

        connector = new KafkaConnector();
        connector.executionHolder = new ExecutionHolder(vertx);
        connector.configurations = UnsatisfiedInstance.instance();
        connector.defaultKafkaConfiguration = UnsatisfiedInstance.instance();
        connector.consumerRebalanceListeners = UnsatisfiedInstance.instance();
        connector.kafkaCDIEvents = new CountKafkaCdiEvents();
        connector.init();

        PublisherBuilder<? extends KafkaRecord> builder = (PublisherBuilder<? extends KafkaRecord>) connector
                .getPublisherBuilder(config);

        List<KafkaRecord> messages1 = new ArrayList<>();
        List<KafkaRecord> messages2 = new ArrayList<>();
        builder.forEach(messages1::add).run();
        builder.forEach(messages2::add).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

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
    public void testRetry() {
        // This test need an individual Kafka container
        try (StrimziKafkaContainer kafka = new StrimziKafkaContainer()) {
            kafka.start();
            await().until(kafka::isRunning);
            MapBasedConfig config = newCommonConfigForSource()
                    .with("bootstrap.servers", kafka.getBootstrapServers())
                    .with("value.deserializer", IntegerDeserializer.class.getName())
                    .with("retry", true)
                    .with("retry-attempts", 100)
                    .with("retry-max-wait", 30);

            usage.setBootstrapServers(kafka.getBootstrapServers());

            KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
            source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                    UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents,
                    UnsatisfiedInstance.instance(), -1);
            List<KafkaRecord> messages1 = new ArrayList<>();
            source.getStream().subscribe().with(messages1::add);

            AtomicInteger counter = new AtomicInteger();
            new Thread(() -> usage.produceIntegers(10, null,
                    () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

            await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

            try (@SuppressWarnings("unused")
            FixedKafkaContainer container = restart(kafka, 2)) {
                new Thread(() -> usage.produceIntegers(10, null,
                        () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

                await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
                assertThat(messages1.size()).isGreaterThanOrEqualTo(20);
            }
        }
    }

    private KafkaMapBasedConfig myKafkaSourceConfig(int partitions, String withConsumerRebalanceListener, String group) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.incoming.data");
        if (group != null) {
            builder.put("group.id", group);
        }
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", "data");
        if (partitions > 0) {
            builder.put("partitions", Integer.toString(partitions));
            builder.put("topic", "data-" + partitions);
        }
        if (withConsumerRebalanceListener != null) {
            builder.put("consumer-rebalance-listener.name", withConsumerRebalanceListener);
        }

        return builder.build();
    }

    private KafkaMapBasedConfig myKafkaSourceConfigWithoutAck(String suffix, boolean shorterTimeouts) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.incoming.data");
        builder.put("group.id", "my-group-starting-on-fifth-" + suffix);
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", "data-starting-on-fifth-" + suffix);
        if (shorterTimeouts) {
            builder.put("max.poll.interval.ms", "2000");
        }

        return builder.build();
    }

    @Test
    public void testABeanConsumingTheKafkaMessages() {
        ConsumptionBean bean = run(myKafkaSourceConfig(0, null, "my-group"));
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("data", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<KafkaRecord<String, Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            assertThat(m.getTopic()).isEqualTo("data");
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
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesMultiThread() {
        MultiThreadConsumer bean = runApplication(myKafkaSourceConfig(0, null, "my-group")
                .with("mp.messaging.incoming.data.topic", topic), MultiThreadConsumer.class);
        List<Integer> list = bean.getItems();
        assertThat(list).isEmpty();
        bean.run();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(100, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 100);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesWithPartitions() {
        createTopic("data-2", 2);
        ConsumptionBean bean = run(
                myKafkaSourceConfig(2, ConsumptionConsumerRebalanceListener.class.getSimpleName(), null));
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("data-2", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsOnly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<KafkaRecord<String, Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            assertThat(m.getTopic()).isEqualTo("data-2");
            assertThat(m.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(m.getPartition()).isGreaterThan(-1);
        });

        ConsumptionConsumerRebalanceListener consumptionConsumerRebalanceListener = getConsumptionConsumerRebalanceListener();
        assertThat(consumptionConsumerRebalanceListener.getAssigned().size()).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            TopicPartition partition = consumptionConsumerRebalanceListener.getAssigned().get(i);
            assertThat(partition).isNotNull();
            assertThat(partition.topic()).isEqualTo("data-2");
        }
    }

    @Test
    public void testABeanConsumingWithMissingRebalanceListenerConfiguredByName() {
        assertThatThrownBy(() -> run(myKafkaSourceConfig(0, "not exists", "my-group")))
                .isInstanceOf(DeploymentException.class)
                .hasCauseInstanceOf(UnsatisfiedResolutionException.class);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesStartingOnFifthOffsetFromLatest() {
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean callback = new AtomicBoolean(false);
        new Thread(() -> usage.produceIntegers(10, () -> callback.set(true),
                () -> new ProducerRecord<>("data-starting-on-fifth-happy-path", counter.getAndIncrement()))).start();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(callback::get);
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
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean callback = new AtomicBoolean(false);
        new Thread(() -> usage.produceIntegers(10, () -> callback.set(true),
                () -> new ProducerRecord<>("data-starting-on-fifth-fail-on-first-attempt", counter.getAndIncrement())))
                        .start();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(callback::get);
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
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean callback = new AtomicBoolean(false);
        new Thread(() -> usage.produceIntegers(10, () -> callback.set(true),
                () -> new ProducerRecord<>("data-starting-on-fifth-fail-until-second-rebalance",
                        counter.getAndIncrement())))
                                .start();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(callback::get);
        /*
         * Will use StartFromFifthOffsetFromLatestConsumerRebalanceListener
         */
        ConsumptionBeanWithoutAck bean = runWithoutAck(
                myKafkaSourceConfigWithoutAck("fail-until-second-rebalance", true));
        List<Integer> list = bean.getResults();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> list.size() >= 5);

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

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(2, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 2);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1);

        new Thread(() -> usage.produceStrings(1, null, () -> new ProducerRecord<>(topic, "hello"))).start();

        new Thread(() -> usage.produceIntegers(2, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        // no other message received
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testABeanConsumingTheKafkaMessagesWithRawMessage() {
        ConsumptionBeanUsingRawMessage bean = runApplication(myKafkaSourceConfig(0, null, "my-group"),
                ConsumptionBeanUsingRawMessage.class);
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("data", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<Message<Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            IncomingKafkaRecordMetadata<String, Integer> metadata = m.getMetadata(IncomingKafkaRecordMetadata.class)
                    .orElseThrow(() -> new AssertionError("Metadata expected"));
            assertThat(metadata.getTopic()).isEqualTo("data");
            assertThat(metadata.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(metadata.getPartition()).isGreaterThan(-1);
            assertThat(metadata.getOffset()).isGreaterThan(-1);
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

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private ConsumptionConsumerRebalanceListener getConsumptionConsumerRebalanceListener() {
        return getBeanManager()
                .createInstance()
                .select(ConsumptionConsumerRebalanceListener.class)
                .select(NamedLiteral.of(ConsumptionConsumerRebalanceListener.class.getSimpleName()))
                .get();

    }

    private StartFromFifthOffsetFromLatestConsumerRebalanceListener getStartFromFifthOffsetFromLatestConsumerRebalanceListener(
            String name) {
        return getBeanManager()
                .createInstance()
                .select(StartFromFifthOffsetFromLatestConsumerRebalanceListener.class)
                .select(NamedLiteral.of(name))
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
