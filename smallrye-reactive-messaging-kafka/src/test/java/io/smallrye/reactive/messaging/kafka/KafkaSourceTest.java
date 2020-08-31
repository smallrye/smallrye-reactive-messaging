package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import javax.enterprise.inject.spi.BeanManager;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DeploymentException;
import org.jboss.weld.exceptions.UnsatisfiedResolutionException;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.client.common.TopicPartition;

public class KafkaSourceTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSource() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                getConsumerRebalanceListeners());

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSourceWithPartitions() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("partitions", 4);

        kafka.createTopic(topic, 3, 1);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                getConsumerRebalanceListeners());

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
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("channel-name", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                getConsumerRebalanceListeners());

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
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("broadcast", true);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnector connector = new KafkaConnector();
        connector.executionHolder = new ExecutionHolder(vertx);
        connector.defaultKafkaConfiguration = UnsatisfiedInstance.instance();
        connector.consumerRebalanceListeners = getConsumerRebalanceListeners();
        connector.init();
        PublisherBuilder<? extends KafkaRecord> builder = (PublisherBuilder<? extends KafkaRecord>) connector
                .getPublisherBuilder(new MapBasedConfig(config));

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
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testBroadcastWithPartitions() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        kafka.createTopic(topic, 2, 1);
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("broadcast", true);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("partitions", 2);
        KafkaConnector connector = new KafkaConnector();
        connector.executionHolder = new ExecutionHolder(vertx);
        connector.defaultKafkaConfiguration = UnsatisfiedInstance.instance();
        connector.consumerRebalanceListeners = getConsumerRebalanceListeners();
        connector.init();
        PublisherBuilder<? extends KafkaRecord> builder = (PublisherBuilder<? extends KafkaRecord>) connector
                .getPublisherBuilder(new MapBasedConfig(config));

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
    public void testRetry() throws IOException, InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("retry", true);
        config.put("retry-attempts", 100);
        config.put("retry-max-wait", 30);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                getConsumerRebalanceListeners());
        List<KafkaRecord> messages1 = new ArrayList<>();
        source.getStream().subscribe().with(messages1::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

        restart(5);

        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
        assertThat(messages1.size()).isGreaterThanOrEqualTo(20);
    }

    private Map<String, Object> newCommonConfig() {
        String randomId = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", randomId);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("tracing-enabled", false);
        return config;
    }

    private MapBasedConfig myKafkaSourceConfig(int partitions, String withConsumerRebalanceListener, String group) {
        MapBasedConfig.ConfigBuilder builder = new MapBasedConfig.ConfigBuilder("mp.messaging.incoming.data");
        builder.put("connector", KafkaConnector.CONNECTOR_NAME);
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

        return new MapBasedConfig(builder.build());
    }

    private MapBasedConfig myKafkaSourceConfigWithoutAck(String suffix, boolean shorterTimeouts) {
        MapBasedConfig.ConfigBuilder builder = new MapBasedConfig.ConfigBuilder("mp.messaging.incoming.data");
        builder.put("connector", KafkaConnector.CONNECTOR_NAME);
        builder.put("group.id", "my-group-starting-on-fifth-" + suffix);
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", "data-starting-on-fifth-" + suffix);
        if (shorterTimeouts) {
            builder.put("max.poll.interval.ms", "2000");
        }

        return new MapBasedConfig(builder.build());
    }

    @Test
    public void testABeanConsumingTheKafkaMessages() {
        ConsumptionBean bean = deploy(myKafkaSourceConfig(0, null, "my-group"));
        KafkaUsage usage = new KafkaUsage();
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

        HealthReport liveness = getHealth(container).getLiveness();
        HealthReport readiness = getHealth(container).getReadiness();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("data");
        assertThat(readiness.getChannels().get(0).getChannel()).isEqualTo("data");
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesWithPartitions() {
        kafka.createTopic("data-2", 2, 1);
        ConsumptionBean bean = deploy(myKafkaSourceConfig(2, ConsumptionConsumerRebalanceListener.class.getSimpleName(), null));
        KafkaUsage usage = new KafkaUsage();
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("data-2", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<KafkaRecord<String, Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            assertThat(m.getTopic()).isEqualTo("data-2");
            assertThat(m.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(m.getPartition()).isGreaterThan(-1);
        });

        ConsumptionConsumerRebalanceListener consumptionConsumerRebalanceListener = getConsumptionConsumerRebalanceListener();
        assertThat(consumptionConsumerRebalanceListener.getAssigned().size()).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            TopicPartition topicPartition = consumptionConsumerRebalanceListener.getAssigned().get(i);
            assertThat(topicPartition).isNotNull();
            assertThat(topicPartition.getTopic()).isEqualTo("data-2");
        }
    }

    @Test(expected = UnsatisfiedResolutionException.class)
    public void testABeanConsumingWithMissingRebalanceListenerConfiguredByName() throws Throwable {
        try {
            deploy(myKafkaSourceConfig(0, "not exists", "my-group"));
        } catch (DeploymentException ex) {
            throw ex.getCause();
        }
        fail("Should've failed to resolve 'not exists' consumer rebalance listener");
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesStartingOnFifthOffsetFromLatest() {
        KafkaUsage usage = new KafkaUsage();
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
        ConsumptionBeanWithoutAck bean = deployWithoutAck(
                myKafkaSourceConfigWithoutAck("happy-path", false));
        List<Integer> list = bean.getResults();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> list.size() >= 5);

        assertThat(list).containsExactly(6, 7, 8, 9, 10);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesStartingOnFifthOffsetFromLatestThatFailsOnTheFirstAttempt() {
        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean callback = new AtomicBoolean(false);
        new Thread(() -> usage.produceIntegers(10, () -> callback.set(true),
                () -> new ProducerRecord<>("data-starting-on-fifth-fail-on-first-attempt", counter.getAndIncrement()))).start();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(callback::get);
        /*
         * Will use StartFromFifthOffsetFromLatestConsumerRebalanceListener
         */
        ConsumptionBeanWithoutAck bean = deployWithoutAck(
                myKafkaSourceConfigWithoutAck("fail-on-first-attempt", false));
        List<Integer> list = bean.getResults();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> list.size() >= 5);

        assertThat(list).containsExactly(6, 7, 8, 9, 10);

        assertThat(
                getStartFromFifthOffsetFromLatestConsumerRebalanceListener("my-group-starting-on-fifth-fail-on-first-attempt")
                        .getRebalanceCount()).isEqualTo(1);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesStartingOnFifthOffsetFromLatestThatFailsUntilSecondRebalance() {
        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean callback = new AtomicBoolean(false);
        new Thread(() -> usage.produceIntegers(10, () -> callback.set(true),
                () -> new ProducerRecord<>("data-starting-on-fifth-fail-until-second-rebalance", counter.getAndIncrement())))
                        .start();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(callback::get);
        /*
         * Will use StartFromFifthOffsetFromLatestConsumerRebalanceListener
         */
        ConsumptionBeanWithoutAck bean = deployWithoutAck(
                myKafkaSourceConfigWithoutAck("fail-until-second-rebalance", true));
        List<Integer> list = bean.getResults();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> list.size() >= 5);

        assertThat(list).containsExactly(6, 7, 8, 9, 10);

        assertThat(getStartFromFifthOffsetFromLatestConsumerRebalanceListener(
                "my-group-starting-on-fifth-fail-until-second-rebalance").getRebalanceCount()).isEqualTo(2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInvalidIncomingType() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                getConsumerRebalanceListeners());

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
        ConsumptionBeanUsingRawMessage bean = deployRaw(myKafkaSourceConfig(0, null, "my-group"));
        KafkaUsage usage = new KafkaUsage();
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

        HealthReport liveness = getHealth(container).getLiveness();
        HealthReport readiness = getHealth(container).getReadiness();
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
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("sasl.jaas.config", ""); //optional configuration
        config.put("sasl.mechanism", ""); //optional configuration
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                getConsumerRebalanceListeners());

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private BeanManager getBeanManager() {
        if (container == null) {
            Weld weld = baseWeld();
            addConfig(new MapBasedConfig(new HashMap<>()));
            weld.disableDiscovery();
            container = weld.initialize();
        }
        return container.getBeanManager();
    }

    private Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return getBeanManager()
                .createInstance()
                .select(KafkaConsumerRebalanceListener.class);
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

    private ConsumptionBean deploy(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ConsumptionConsumerRebalanceListener.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
    }

    private ConsumptionBeanWithoutAck deployWithoutAck(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(ConsumptionBeanWithoutAck.class);
        weld.addBeanClass(ConsumptionConsumerRebalanceListener.class);
        weld.addBeanClass(StartFromFifthOffsetFromLatestConsumerRebalanceListener.class);
        weld.addBeanClass(StartFromFifthOffsetFromLatestButFailOnFirstConsumerRebalanceListener.class);
        weld.addBeanClass(StartFromFifthOffsetFromLatestButFailUntilSecondRebalanceConsumerRebalanceListener.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBeanWithoutAck.class).get();
    }

    private ConsumptionBeanUsingRawMessage deployRaw(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(ConsumptionBeanUsingRawMessage.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBeanUsingRawMessage.class).get();
    }

}
