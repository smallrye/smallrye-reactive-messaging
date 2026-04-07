package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Verify backward compatibility: default mqtt-version=4 works unchanged,
 * v5 metadata accessors return null on v3.1.1 messages.
 */
public class MqttBackwardCompatibilityTest extends MqttTestBase {

    @AfterEach
    public void cleanup() {
        Clients.clear();
    }

    @Test
    public void testDefaultVersionIs4() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        // No mqtt-version set — should default to 4
        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)),
                null, null);

        List<MqttMessage<?>> messages = new ArrayList<>();
        Flow.Publisher<? extends MqttMessage<?>> stream = source.getSource();
        Multi.createFrom().publisher(stream).subscribe().with(messages::add);
        awaitUntilReady(source);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 3, null, counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 3);
        assertThat(messages).hasSize(3);
    }

    @Test
    public void testV5MetadataAccessorsReturnNullOnV31Messages() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        // Using v3.1.1 (default)
        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)),
                null, null);

        List<ReceivingMqttMessage> messages = new ArrayList<>();
        Flow.Publisher<ReceivingMqttMessage> stream = (Flow.Publisher<ReceivingMqttMessage>) source.getSource();
        Multi.createFrom().publisher(stream).subscribe().with(messages::add);
        awaitUntilReady(source);

        // Publish using v3.1.1 client (no v5 properties)
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 1, null, counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        ReceivingMqttMessage msg = messages.get(0);
        ReceivingMqttMessageMetadata metadata = msg.getMetadata(ReceivingMqttMessageMetadata.class).orElseThrow();

        // v5 properties should be null on v3.1.1 messages
        assertThat(metadata.getContentType()).isNull();
        assertThat(metadata.getResponseTopic()).isNull();
        assertThat(metadata.getCorrelationData()).isNull();
        assertThat(metadata.getMessageExpiryInterval()).isNull();
        assertThat(metadata.getPayloadFormatIndicator()).isNull();
        assertThat(metadata.getSubscriptionIdentifier()).isNull();
    }
}
