package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;

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
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, new MapBasedConfig(config), SERVERS);

        List<Message<?>> messages = new ArrayList<>();
        source.getSource().forEach(messages::add).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaMessage<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testSourceWithChannelName() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("channel-name", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, new MapBasedConfig(config), SERVERS);

        List<KafkaMessage> messages = new ArrayList<>();
        source.getSource().forEach(m -> messages.add((KafkaMessage) m)).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(KafkaMessage::getPayload).collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4,
                5, 6, 7, 8, 9);
    }

    @Test
    public void testBroadcast() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("broadcast", true);
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, new MapBasedConfig(config), SERVERS);

        List<KafkaMessage> messages1 = new ArrayList<>();
        List<KafkaMessage> messages2 = new ArrayList<>();
        source.getSource().forEach(m -> messages1.add((KafkaMessage) m)).run();
        source.getSource().forEach(m -> messages2.add((KafkaMessage) m)).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream().map(KafkaMessage::getPayload).collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4,
                5, 6, 7, 8, 9);
        assertThat(messages2.stream().map(KafkaMessage::getPayload).collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4,
                5, 6, 7, 8, 9);
    }

    @Test
    public void testRetry() throws IOException, InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("retry", true);
        config.put("retry-attempts", 100);

        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, new MapBasedConfig(config), SERVERS);
        List<KafkaMessage> messages1 = new ArrayList<>();
        source.getSource().forEach(m -> messages1.add((KafkaMessage) m)).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

        restart(5);

        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
    }

    private Map<String, Object> newCommonConfig() {
        String randomId = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", randomId);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        return config;
    }

    private MapBasedConfig myKafkaSourceConfig() {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "topic", "data");

        return new MapBasedConfig(config);
    }

    @Test
    public void testABeanConsumingTheKafkaMessages() {
        ConsumptionBean bean = deploy(myKafkaSourceConfig());
        KafkaUsage usage = new KafkaUsage();
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("data", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<KafkaMessage<String, Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            assertThat(m.getTopic()).isEqualTo("data");
            assertThat(m.getTimestamp()).isGreaterThan(0);
            assertThat(m.getPartition()).isGreaterThan(-1);
            assertThat(m.getOffset()).isGreaterThan(-1);
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInvalidIncomingType() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, new MapBasedConfig(config), SERVERS);

        List<Message<?>> messages = new ArrayList<>();
        source.getSource().forEach(messages::add).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(2, null,
            () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 2);
        assertThat(messages.stream().map(m -> ((KafkaMessage<String, Integer>) m).getPayload())
            .collect(Collectors.toList())).containsExactly(0, 1);

        new Thread(() -> usage.produceStrings(1, null, () -> new ProducerRecord<>(topic, "hello"))).start();

        new Thread(() -> usage.produceIntegers(2, null,
            () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        // no other message received
        assertThat(messages.stream().map(m -> ((KafkaMessage<String, Integer>) m).getPayload())
            .collect(Collectors.toList())).containsExactly(0, 1);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesWithRawMessage() {
        ConsumptionBeanUsingRawMessage bean = deployRaw(myKafkaSourceConfig());
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
            Metadata metadata = m.getMetadata();
            assertThat(metadata.getAsString(KafkaMetadata.TOPIC, null)).isEqualTo("data");
            assertThat(metadata.getAsLong(KafkaMetadata.TIMESTAMP, -1L)).isGreaterThan(0);
            assertThat(metadata.getAsInteger(KafkaMetadata.PARTITION, -1)).isGreaterThan(-1);
            assertThat(metadata.getAsInteger(KafkaMetadata.OFFSET, -1)).isGreaterThan(-1);
        });
    }

    private ConsumptionBean deploy(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(ConsumptionBean.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
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
