package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertThrows;

import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MutualTlsMqttSourceTest extends MutualTlsMqttTestBase {

    @AfterEach
    public void cleanup() {
        Clients.clear();
    }

    @Test
    public void testMutualTLS() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("username", "user");
        config.put("password", "foo");
        config.put("channel-name", topic);
        config.put("ssl", true);
        config.put("ssl.keystore.type", "jks");
        config.put("ssl.keystore.location", "mosquitto-tls/client/client.ks");
        config.put("ssl.keystore.password", "password");
        config.put("ssl.truststore.type", "jks");
        config.put("ssl.truststore.location", "mosquitto-tls/client/client.ts");
        config.put("ssl.truststore.password", "password");

        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)),
                null);

        List<MqttMessage<?>> messages = new ArrayList<>();
        Flow.Publisher<? extends MqttMessage<?>> stream = source.getSource();
        Multi.createFrom().publisher(stream).subscribe().with(messages::add);
        awaitUntilReady(source);
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

    void pause() {
        // TODO To be removed - there is a race between the subscription and the consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testMutualTLSMissingPassword() {
        assertThrows(IllegalArgumentException.class, () -> {
            String topic = UUID.randomUUID().toString();
            Map<String, Object> config = new HashMap<>();
            config.put("topic", topic);
            config.put("host", address);
            config.put("port", port);
            config.put("username", "user");
            config.put("password", "foo");
            config.put("channel-name", topic);
            config.put("ssl", true);
            config.put("ssl.keystore.type", "jks");
            config.put("ssl.keystore.location", "mosquitto-tls/client/client.ks");
            config.put("ssl.truststore.type", "jks");
            config.put("ssl.truststore.location", "mosquitto-tls/client/client.ts");

            new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)), null);
        });

    }

}
