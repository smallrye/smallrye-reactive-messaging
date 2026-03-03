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

    // --- getOrCreateAdminClient tests ---

    @Test
    void getOrCreateDeduplicatesSameConfig() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config);

        assertThat(admin1).isSameAs(admin2);
    }

    @Test
    void getOrCreateSeparatesForDifferentConfig() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();

        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://" + companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config1);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config2);

        assertThat(admin1).isNotSameAs(admin2);
    }

    // --- release tests ---

    @Test
    void releaseClosesAdminWhenLastReference() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config);

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

        Map<String, Object> config1 = new HashMap<>();
        config1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        Map<String, Object> config2 = new HashMap<>();
        config2.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://" + companion.getBootstrapServers());

        KafkaAdmin admin1 = registry.getOrCreateAdminClient(config1);
        KafkaAdmin admin2 = registry.getOrCreateAdminClient(config2);

        registry.close();

        assertThatThrownBy(() -> admin1.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> admin2.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void releaseThenCloseAllDoesNotDoubleClose() {
        KafkaAdminClientRegistry registry = new KafkaAdminClientRegistry();

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());

        KafkaAdmin admin = registry.getOrCreateAdminClient(config);

        // Release removes and closes it
        registry.release(admin);
        assertThatThrownBy(() -> admin.describeCluster().await().indefinitely())
                .isInstanceOf(IllegalStateException.class);

        // Bulk close should not close it again since it was already removed
        registry.close();
    }

}
