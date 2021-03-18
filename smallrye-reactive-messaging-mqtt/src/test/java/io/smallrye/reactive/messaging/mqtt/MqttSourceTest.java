package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MqttSourceTest extends MqttTestBase {

    protected WeldContainer container;

    private MqttFactory vertxMqttFactory = new VertxMqttFactory();

    protected MqttFactory mqttFactory() {
        return vertxMqttFactory;
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        mqttFactory().clearClients();
    }

    @Test
    public void testSource() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        Source source = mqttFactory().createSource(vertx, config);

        List<MqttMessage<?>> messages = new ArrayList<>();
        PublisherBuilder<MqttMessage<?>> stream = source.getSource();
        stream.forEach(messages::add).run();
        await().until(source::isSubscribed);
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 10, null,
                counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream()
                .map(Message::getPayload)
                .map(x -> (byte[]) x)
                .map(bytes -> Integer.valueOf(new String(bytes)))
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testSourceUsingChannelName() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("host", address);
        config.put("port", port);
        Source source = mqttFactory().createSource(vertx, config);

        List<MqttMessage<?>> messages = new ArrayList<>();
        PublisherBuilder<MqttMessage<?>> stream = source.getSource();
        stream.forEach(messages::add).run();
        await().until(source::isSubscribed);
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 10, null,
                counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream()
                .map(Message::getPayload)
                .map(x -> (byte[]) x)
                .map(bytes -> Integer.valueOf(new String(bytes)))
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testBroadcast() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        config.put("broadcast", true);

        Source source = mqttFactory().createSource(vertx, config);

        List<MqttMessage<?>> messages1 = new ArrayList<>();
        List<MqttMessage<?>> messages2 = new ArrayList<>();
        PublisherBuilder<MqttMessage<?>> stream = source.getSource();
        stream.forEach(messages1::add).run();
        stream.forEach(messages2::add).run();

        await().until(source::isSubscribed);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 10, null,
                counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream()
                .map(Message::getPayload)
                .map(x -> (byte[]) x)
                .map(bytes -> Integer.valueOf(new String(bytes)))
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(messages2.stream()
                .map(Message::getPayload)
                .map(x -> (byte[]) x)
                .map(bytes -> Integer.valueOf(new String(bytes)))
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testWithVeryLargeMessage() {
        Random random = new Random();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        config.put("max-message-size", 20 * 1024);
        Source source = mqttFactory().createSource(vertx, config);

        byte[] large = new byte[10 * 1024];
        random.nextBytes(large);

        List<MqttMessage<?>> messages = new ArrayList<>();
        PublisherBuilder<MqttMessage<?>> stream = source.getSource();
        stream.forEach(messages::add).run();
        await().until(source::isSubscribed);
        new Thread(() -> usage.produce(topic, 10, null,
                () -> large)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream()
                .map(Message::getPayload)
                .map(x -> (byte[]) x)
                .collect(Collectors.toList()))
                        .contains(large);
    }

    public static MapBasedConfig getConfigForConnector(String connectorName) {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "topic", "data");
        config.put(prefix + "connector", connectorName);
        config.put(prefix + "host", System.getProperty("mqtt-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        if (System.getProperty("mqtt-user") != null) {
            config.put(prefix + "username", System.getProperty("mqtt-user"));
            config.put(prefix + "password", System.getProperty("mqtt-pwd"));
        }
        return new MapBasedConfig(config);
    }

    @Test
    public void testABeanConsumingTheMQTTMessages() {
        ConsumptionBean bean = deploy();

        await()
                .until(() -> mqttFactory().connectorIsReady(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers("data", 10, null, counter::getAndIncrement))
                .start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    private ConsumptionBean deploy() {
        Weld weld = baseWeld(getConfigForConnector(mqttFactory().connectorName()));
        weld.addBeanClass(ConsumptionBean.class);
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
    }

}
