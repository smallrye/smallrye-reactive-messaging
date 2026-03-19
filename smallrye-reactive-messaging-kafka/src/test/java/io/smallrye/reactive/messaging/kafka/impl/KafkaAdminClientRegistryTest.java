package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.KafkaAdmin;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;

class KafkaAdminClientRegistryTest extends KafkaCompanionTestBase {

    // --- non-pooled (default) tests ---

    @Test
    void nonPooledCreatesSeparateInstances() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config, "channel-a", true);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config, "channel-b", true);

        assertThat(admin1).isNotSameAs(admin2);

        // Both should work independently
        assertThat(admin1.describeCluster().await().indefinitely()).isNotNull();
        assertThat(admin2.describeCluster().await().indefinitely()).isNotNull();

        // Release one, the other still works
        registry.release(admin1);
        assertThatThrownBy(() -> admin1.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);
        assertThat(admin2.describeCluster().await().indefinitely()).isNotNull();

        registry.release(admin2);
        assertThatThrownBy(() -> admin2.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);
    }

    // --- pooled mode tests ---

    @Test
    void pooledDeduplicatesSameConfig() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();
        registry.poolingEnabled = true;

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config, "channel-a", true);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config, "channel-b", true);

        assertThat(admin1).isSameAs(admin2);
    }

    @Test
    void pooledSeparatesForDifferentConfig() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();
        registry.poolingEnabled = true;

        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://" + companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config1, "channel-a", true);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config2, "channel-b", true);

        assertThat(admin1).isNotSameAs(admin2);
    }

    // --- release tests ---

    @Test
    void pooledReleaseClosesAdminWhenLastReference() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();
        registry.poolingEnabled = true;

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config, "channel-a", true);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config, "channel-b", true);

        assertThat(admin1).isSameAs(admin2);
        assertThat(admin1.describeCluster().await().indefinitely()).isNotNull();

        // Release first reference — admin still has one reference
        registry.release(admin1);
        assertThat(admin1.describeCluster().await().indefinitely()).isNotNull();

        // Release second reference — admin no longer referenced
        registry.release(admin2);
        assertThatThrownBy(() -> admin1.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void releaseNullIsNoOp() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();
        // Should not throw
        registry.release(null);
    }

    // --- close() tests ---

    @Test
    void closeAllClosesEverything() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();
        registry.poolingEnabled = true;

        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://" + companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config1, "channel-a", true);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config2, "channel-b", false);

        registry.close();

        assertThatThrownBy(() -> admin1.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> admin2.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void pooledReleaseThenCloseAllDoesNotDoubleClose() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();
        registry.poolingEnabled = true;

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        KafkaAdmin admin = registry.getOrCreateAdminClient(config, "channel-a", true);

        // Release removes and closes it
        registry.release(admin);
        assertThatThrownBy(() -> admin.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);

        // Bulk close should not close it again since it was already removed
        registry.close();
    }

    // --- adminClientName tests ---

    @Test
    void adminClientNameWithClientId() {
        Map<String, Object> config = new HashMap<>();
        config.put("client.id", "my-client");

        assertThat(KafkaAdminClientRegistry.adminClientName(config, "my-channel", true))
                .isEqualTo("kafka-admin-my-client-my-channel");
        assertThat(KafkaAdminClientRegistry.adminClientName(config, "my-channel", false))
                .isEqualTo("kafka-admin-my-client-my-channel");
    }

    @Test
    void adminClientNameWithoutClientId() {
        Map<String, Object> config = new HashMap<>();

        assertThat(KafkaAdminClientRegistry.adminClientName(config, "prices", true))
                .isEqualTo("kafka-admin-incoming-prices");
        assertThat(KafkaAdminClientRegistry.adminClientName(config, "orders", false))
                .isEqualTo("kafka-admin-outgoing-orders");
    }

}
