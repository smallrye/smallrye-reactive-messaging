package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.MqttSourceTest.getConfigForConnector;
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
import org.junit.After;
import org.junit.Test;

public class SecureMqttSourceTest extends SecureMqttTestBase {

    private WeldContainer container;

    private MqttFactory vertxMqttFactory = new VertxMqttFactory();

    protected MqttFactory mqttFactory() {
        return vertxMqttFactory;
    }

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    @Test
    public void testSecureSource() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("username", "user");
        config.put("password", "foo");
        config.put("channel-name", topic);
        Source source = mqttFactory().createSource(vertx, config);

        List<MqttMessage<?>> messages = new ArrayList<>();
        PublisherBuilder<MqttMessage<?>> stream = source.getSource();
        stream.forEach(messages::add).run();
        await().until(source::isSubscribed);
        pause();
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
    public void testABeanConsumingTheMQTTMessagesWithAuthentication() {
        ConsumptionBean bean = deploy();
        pause();
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers("data", 10, null, counter::getAndIncrement))
                .start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    void pause() {
        // TODO To be removed - there is a race between the subscription and the consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ConsumptionBean deploy() {
        Weld weld = MqttTestBase.baseWeld(getConfigForConnector(mqttFactory().connectorName()));
        weld.addBeanClass(ConsumptionBean.class);
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
    }

}
