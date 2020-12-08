package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class DynamicMqttTopicSourceTest extends MqttTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
    }

    private void awaitAndVerify() {
        DynamicTopicApp bean = container.getBeanManager().createInstance().select(DynamicTopicApp.class).get();
        MqttConnector connector = this.container
                .select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get();

        await()
                .pollInterval(Duration.ofSeconds(1))
                .until(connector::isSourceReady);

        bean.publish();

        await().until(() -> bean.messages().size() >= 3);

        assertThat(bean.messages()).allSatisfy(m -> {
            assertThat(m.getTopic()).matches("/app/hello/mqtt-.*/greeting");
            assertThat(new String(m.getPayload())).startsWith("hello from dynamic topic ");
        });
    }

    @Test
    public void testWithDash() {
        Weld weld = baseWeld(getConfig("#"));
        weld.addBeanClass(DynamicTopicApp.class);

        container = weld.initialize();

        awaitAndVerify();
    }

    @Test
    public void testWithDashInExpression() {
        Weld weld = baseWeld(getConfig("/app/#"));
        weld.addBeanClass(DynamicTopicApp.class);

        container = weld.initialize();

        awaitAndVerify();
    }

    @Test
    public void testWithPlusInExpression() {
        Weld weld = baseWeld(getConfig("/app/hello/+/greeting"));
        weld.addBeanClass(DynamicTopicApp.class);

        container = weld.initialize();

        awaitAndVerify();
    }

    @Test
    public void testWithTwoPlusInExpression() {
        Weld weld = baseWeld(getConfig("/+/hello/+/greeting"));
        weld.addBeanClass(DynamicTopicApp.class);

        container = weld.initialize();

        awaitAndVerify();
    }

    @Test
    public void testWithTwoPlusAndDashInExpression() {
        Weld weld = baseWeld(getConfig("/+/hello/+/#"));
        weld.addBeanClass(DynamicTopicApp.class);

        container = weld.initialize();

        awaitAndVerify();
    }

    private MapBasedConfig getConfig(String pattern) {
        String prefix = "mp.messaging.outgoing.out.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "topic", "not-used");
        config.put(prefix + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(prefix + "host", System.getProperty("mqtt-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        if (System.getProperty("mqtt-user") != null) {
            config.put(prefix + "username", System.getProperty("mqtt-user"));
            config.put(prefix + "password", System.getProperty("mqtt-pwd"));
        }

        prefix = "mp.messaging.incoming.mqtt.";
        config.put(prefix + "topic", pattern);
        config.put(prefix + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(prefix + "host", System.getProperty("mqtt-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        if (System.getProperty("mqtt-user") != null) {
            config.put(prefix + "username", System.getProperty("mqtt-user"));
            config.put(prefix + "password", System.getProperty("mqtt-pwd"));
        }
        return new MapBasedConfig(config);
    }

    @ApplicationScoped
    public static class DynamicTopicApp {

        private final List<MqttMessage<byte[]>> messages = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("out")
        Emitter<String> emitter;

        public void publish() {
            emitter.send(MqttMessage
                    .of("/app/hello/mqtt-" + LocalDate.now().toString() + "/greeting", "hello from dynamic topic 1",
                            MqttQoS.EXACTLY_ONCE));
            emitter.send(MqttMessage
                    .of("/app/hello/mqtt-" + LocalDate.now().toString() + "/greeting", "hello from dynamic topic 2",
                            MqttQoS.EXACTLY_ONCE));
            emitter.send(MqttMessage
                    .of("/app/hello/mqtt-" + LocalDate.now().toString() + "/greeting", "hello from dynamic topic 3",
                            MqttQoS.EXACTLY_ONCE));
        }

        @Incoming("mqtt")
        public CompletionStage<Void> received(MqttMessage<byte[]> message) {
            messages.add(message);
            return message.ack();
        }

        public List<MqttMessage<byte[]>> messages() {
            return messages;
        }

    }
}
