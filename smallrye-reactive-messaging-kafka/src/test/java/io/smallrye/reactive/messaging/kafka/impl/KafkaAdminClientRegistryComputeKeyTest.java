package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

public class KafkaAdminClientRegistryComputeKeyTest {

    private static String keyFor(Map<String, Object> config) {
        return KafkaAdminClientRegistry.computeKey(KafkaAdminClientRegistry.normalizeAdminConfig(config));
    }

    @Test
    void computeKeySameConfigSameKey() {
        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        assertThat(keyFor(config1)).isEqualTo(keyFor(config2));
    }

    @Test
    void computeKeyDifferentConfigDifferentKey() {
        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "other-host:9092");

        assertThat(keyFor(config1)).isNotEqualTo(keyFor(config2));
    }

    @Test
    void computeKeyExcludesClientId() {
        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config1.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-1");

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config2.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-2");

        assertThat(keyFor(config1)).isEqualTo(keyFor(config2));
    }

    @Test
    void computeKeyAppliesDefaultBackoff() {
        Map<String, Object> withoutBackoff = new HashMap<>();
        withoutBackoff.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Map<String, Object> withDefaultBackoff = new HashMap<>();
        withDefaultBackoff.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        withDefaultBackoff.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");

        assertThat(keyFor(withoutBackoff)).isEqualTo(keyFor(withDefaultBackoff));
    }

    @Test
    void computeKeyIncludesSaslProperties() {
        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config1.put("sasl.mechanism", "PLAIN");

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        assertThat(keyFor(config1)).isNotEqualTo(keyFor(config2));
    }

    @Test
    void computeKeyIgnoresNonAdminProperties() {
        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config1.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        assertThat(keyFor(config1)).isEqualTo(keyFor(config2));
    }

    @Test
    void computeKeyIsSha256() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        String key = keyFor(config);
        // SHA-256 hex is 64 characters
        assertThat(key).hasSize(64).matches("[0-9a-f]+");
    }
}
