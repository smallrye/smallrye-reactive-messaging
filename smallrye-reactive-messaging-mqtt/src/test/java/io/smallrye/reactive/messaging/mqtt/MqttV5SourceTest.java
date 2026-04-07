package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MqttV5SourceTest extends MqttTestBase {

    private MqttV5Usage v5usage;

    @BeforeEach
    @Override
    public void setup() {
        super.setup();
        v5usage = new MqttV5Usage(address, port);
    }

    @AfterEach
    public void cleanup() {
        Clients.clear();
        if (v5usage != null) {
            v5usage.close();
        }
    }

    @Test
    public void testSourceWithMqttV5() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        config.put("mqtt-version", 5);
        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)),
                null, null);

        List<MqttMessage<?>> messages = new ArrayList<>();
        Flow.Publisher<? extends MqttMessage<?>> stream = source.getSource();
        Multi.createFrom().publisher(stream).subscribe().with(messages::add);
        awaitUntilReady(source);

        v5usage.produce(topic, 5, null, () -> "hello-v5".getBytes(StandardCharsets.UTF_8));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 5);
        assertThat(messages).hasSize(5);
    }

    @Test
    public void testSourceReceivesV5Properties() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        config.put("mqtt-version", 5);
        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)),
                null, null);

        List<ReceivingMqttMessage> messages = new ArrayList<>();
        Flow.Publisher<ReceivingMqttMessage> stream = (Flow.Publisher<ReceivingMqttMessage>) source.getSource();
        Multi.createFrom().publisher(stream).subscribe().with(messages::add);
        awaitUntilReady(source);

        // Publish with v5 properties
        MqttProperties props = new MqttProperties();
        props.setContentType("application/json");
        props.setResponseTopic("reply/topic");
        props.setCorrelationData("corr-123".getBytes(StandardCharsets.UTF_8));
        props.setMessageExpiryInterval(60L);
        props.setPayloadFormat(true); // true = UTF-8
        List<UserProperty> userProps = new ArrayList<>();
        userProps.add(new UserProperty("key1", "value1"));
        userProps.add(new UserProperty("key2", "value2"));
        props.setUserProperties(userProps);

        v5usage.produce(topic, 1, null, () -> "{\"data\":true}".getBytes(StandardCharsets.UTF_8), props);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        ReceivingMqttMessage msg = messages.get(0);
        ReceivingMqttMessageMetadata metadata = msg.getMetadata(ReceivingMqttMessageMetadata.class).orElseThrow();

        assertThat(metadata.getContentType()).isEqualTo("application/json");
        assertThat(metadata.getResponseTopic()).isEqualTo("reply/topic");
        assertThat(metadata.getCorrelationData()).isEqualTo("corr-123".getBytes(StandardCharsets.UTF_8));
        assertThat(metadata.getPayloadFormatIndicator()).isEqualTo(1);
        assertThat(metadata.getUserProperties())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }
}
