package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;

/**
 * Tests for combined usage of FallbackConfig, OverrideConfig, and PrefixedConfig.
 * These scenarios test real-world use cases like DLQ, delayed retry topics, and request-reply.
 */
class CombinedConfigTest {

    private SmallRyeConfig baseConfig;

    @BeforeEach
    public void setup() {
        Map<String, String> cfg = config();

        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder();
        builder.addDefaultSources();
        builder.withSources(new ConfigSource() {
            @Override
            public Map<String, String> getProperties() {
                return cfg;
            }

            @Override
            public Set<String> getPropertyNames() {
                return cfg.keySet();
            }

            @Override
            public int getOrdinal() {
                return ConfigSource.DEFAULT_ORDINAL;
            }

            @Override
            public String getValue(String s) {
                return cfg.get(s);
            }

            @Override
            public String getName() {
                return "combined-test-config";
            }
        });
        baseConfig = builder.build();
    }

    private static Map<String, String> config() {
        Map<String, String> cfg = new HashMap<>();
        // Main channel config
        cfg.put("mp.messaging.incoming.my-channel.connector", "kafka");
        cfg.put("mp.messaging.incoming.my-channel.topic", "my-topic");
        cfg.put("mp.messaging.incoming.my-channel.bootstrap.servers", "localhost:9092");
        cfg.put("mp.messaging.incoming.my-channel.group.id", "my-group");
        cfg.put("mp.messaging.incoming.my-channel.compression.type", "gzip");
        cfg.put("mp.messaging.incoming.my-channel.acks", "1");
        cfg.put("mp.messaging.incoming.my-channel.linger.ms", "100");

        // DLQ-specific overrides
        cfg.put("mp.messaging.incoming.my-channel.dead-letter-queue.topic", "my-dlq-topic");
        cfg.put("mp.messaging.incoming.my-channel.dead-letter-queue.compression.type", "snappy");

        // Global connector config
        cfg.put("mp.messaging.connector.kafka.default.timeout", "5000");
        cfg.put("mp.messaging.connector.kafka.max.poll.records", "500");
        return cfg;
    }

    @Test
    public void testPrefixedWithFallback() {
        // Simulate DLQ scenario: prefixed config with fallbacks
        Map<String, Object> fallbacks = new HashMap<>();
        fallbacks.put("max.retries", 3);
        fallbacks.put("retry.delay", "1000");

        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");
        FallbackConfig fallbackConfig = new FallbackConfig(connectorConfig, fallbacks);
        PrefixedConfig dlqConfig = new PrefixedConfig(fallbackConfig, "dead-letter-queue");

        // DLQ-specific property (from prefix)
        assertThat(dlqConfig.getValue("topic", String.class)).isEqualTo("my-dlq-topic");

        // Inherited from main channel with DLQ override
        assertThat(dlqConfig.getValue("compression.type", String.class)).isEqualTo("snappy");

        // Inherited from main channel (no DLQ override)
        assertThat(dlqConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
        assertThat(dlqConfig.getValue("acks", String.class)).isEqualTo("1");
        assertThat(dlqConfig.getValue("linger.ms", Integer.class)).isEqualTo(100);

        // From fallback
        assertThat(dlqConfig.getValue("max.retries", Integer.class)).isEqualTo(3);
        assertThat(dlqConfig.getValue("retry.delay", String.class)).isEqualTo("1000");
    }

    @Test
    public void testPrefixedWithOverride() {
        // Simulate DLQ scenario: prefixed config with override functions
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");

        Map<String, Object> overrides = new HashMap<>();
        overrides.put("topic", "overridden-dlq-topic");
        overrides.put("acks", "all");

        Config overrideConfig = Configs.override(connectorConfig, overrides);
        PrefixedConfig dlqConfig = new PrefixedConfig(overrideConfig, "dead-letter-queue");

        // DLQ-specific from config (takes precedence over override)
        assertThat(dlqConfig.getValue("topic", String.class)).isEqualTo("my-dlq-topic");

        // Override applied
        assertThat(dlqConfig.getValue("acks", String.class)).isEqualTo("all");

        // DLQ-specific override from config
        assertThat(dlqConfig.getValue("compression.type", String.class)).isEqualTo("snappy");

        // Inherited from main channel
        assertThat(dlqConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
    }

    @Test
    public void testPrefixedWithOverrideFunctions() {
        // More realistic scenario: override with functions that can access original values
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of(
                        "topic", c -> c.getOriginalValue("topic", String.class).orElse("default") + "-dlq",
                        "group.id", c -> c.getOriginalValue("group.id", String.class).orElse("default") + "-dlq-group"));

        PrefixedConfig dlqConfig = new PrefixedConfig(overrideConfig, "dead-letter-queue");

        // DLQ-specific from config takes precedence over override function
        assertThat(dlqConfig.getValue("topic", String.class)).isEqualTo("my-dlq-topic");

        // Override function applied (no DLQ-specific override in config)
        assertThat(dlqConfig.getValue("group.id", String.class)).isEqualTo("my-group-dlq-group");

        // Inherited from main channel
        assertThat(dlqConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
    }

    @Test
    public void testFallbackWithOverride() {
        // Scenario: override takes precedence over fallback
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");

        Map<String, Object> fallbacks = new HashMap<>();
        fallbacks.put("max.retries", 3);
        fallbacks.put("compression.type", "lz4");

        Map<String, Object> overrides = new HashMap<>();
        overrides.put("compression.type", "zstd");
        overrides.put("acks", "all");

        Config fallbackConfig = Configs.fallback(connectorConfig, fallbacks);
        Config overrideConfig = Configs.override(fallbackConfig, overrides);

        // Override wins
        assertThat(overrideConfig.getValue("compression.type", String.class)).isEqualTo("zstd");
        assertThat(overrideConfig.getValue("acks", String.class)).isEqualTo("all");

        // From fallback (no override)
        assertThat(overrideConfig.getValue("max.retries", Integer.class)).isEqualTo(3);

        // From base config
        assertThat(overrideConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
    }

    @Test
    public void testComplexLayering() {
        // Most complex scenario: Prefix > Override > Fallback > Connector > Base
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");

        Map<String, Object> fallbacks = new HashMap<>();
        fallbacks.put("default.property", "from-fallback");
        fallbacks.put("shared.property", "from-fallback");

        Config fallbackConfig = Configs.fallback(connectorConfig, fallbacks);

        Map<String, Object> overrides = new HashMap<>();
        overrides.put("override.property", "from-override");
        overrides.put("shared.property", "from-override");

        Config overrideConfig = Configs.override(fallbackConfig, overrides);
        Config prefixedConfig = Configs.prefix("dead-letter-queue", overrideConfig);

        // From prefix-specific config (highest priority)
        assertThat(prefixedConfig.getValue("topic", String.class)).isEqualTo("my-dlq-topic");
        assertThat(prefixedConfig.getValue("compression.type", String.class)).isEqualTo("snappy");

        // From override
        assertThat(prefixedConfig.getValue("override.property", String.class)).isEqualTo("from-override");
        assertThat(prefixedConfig.getValue("shared.property", String.class)).isEqualTo("from-override");

        // From fallback
        assertThat(prefixedConfig.getValue("default.property", String.class)).isEqualTo("from-fallback");

        // From connector/base config
        assertThat(prefixedConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
    }

    @Test
    public void testRealWorldDLQScenario() {
        // This simulates the actual DLQ use case from KafkaDeadLetterQueue
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");

        // Simulate what KafkaDeadLetterQueue does
        Config dlqConfig = Configs.prefixOverride(connectorConfig, "dead-letter-queue",
                Map.of(
                        "key.serializer", c -> "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", c -> "org.apache.kafka.common.serialization.StringSerializer",
                        "client.id", c -> "dlq-client-" + c.getOriginalValue("group.id", String.class).orElse("default")));

        // DLQ-specific topic from config
        assertThat(dlqConfig.getValue("topic", String.class)).isEqualTo("my-dlq-topic");

        // DLQ-specific compression from config
        assertThat(dlqConfig.getValue("compression.type", String.class)).isEqualTo("snappy");

        // Override applied
        assertThat(dlqConfig.getValue("key.serializer", String.class))
                .isEqualTo("org.apache.kafka.common.serialization.StringSerializer");
        assertThat(dlqConfig.getValue("value.serializer", String.class))
                .isEqualTo("org.apache.kafka.common.serialization.StringSerializer");
        assertThat(dlqConfig.getValue("client.id", String.class)).isEqualTo("dlq-client-my-group");

        // Inherited from main channel
        assertThat(dlqConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
        assertThat(dlqConfig.getValue("linger.ms", Integer.class)).isEqualTo(100);
    }

    @Test
    public void testDelayedRetryTopicScenario() {
        // Simulate delayed retry topic configuration
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");

        // Producer config for delayed retry topic
        Config retryProducerConfig = Configs.prefixOverride(connectorConfig, "delayed-retry-topic",
                Map.of(
                        "topic", c -> c.getOriginalValue("topic", String.class).orElse("default") + "-retry",
                        "client.id", c -> "retry-producer-" + c.getOriginalValue("group.id", String.class).orElse("default")));

        assertThat(retryProducerConfig.getValue("topic", String.class)).isEqualTo("my-topic-retry");
        assertThat(retryProducerConfig.getValue("client.id", String.class)).isEqualTo("retry-producer-my-group");
        assertThat(retryProducerConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
        assertThat(retryProducerConfig.getValue("compression.type", String.class)).isEqualTo("gzip");

        // Consumer config for delayed retry topic
        Config retryConsumerConfig = Configs.prefixOverride(connectorConfig, "delayed-retry-topic.consumer",
                Map.of(
                        "lazy-client", c -> true,
                        "client.id", c -> "retry-consumer-" + c.getOriginalValue("group.id", String.class).orElse("default"),
                        "group.id", c -> "retry-group-" + c.getOriginalValue("group.id", String.class).orElse("default")));

        assertThat(retryConsumerConfig.getValue("lazy-client", Boolean.class)).isTrue();
        assertThat(retryConsumerConfig.getValue("client.id", String.class)).isEqualTo("retry-consumer-my-group");
        assertThat(retryConsumerConfig.getValue("group.id", String.class)).isEqualTo("retry-group-my-group");
        assertThat(retryConsumerConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
    }

    @Test
    public void testRequestReplyScenario() {
        // Simulate request-reply configuration
        Map<String, String> cfg = new HashMap<>();
        cfg.put("mp.messaging.outgoing.requests.connector", "kafka");
        cfg.put("mp.messaging.outgoing.requests.topic", "request-topic");
        cfg.put("mp.messaging.outgoing.requests.bootstrap.servers", "localhost:9092");
        cfg.put("mp.messaging.outgoing.requests.reply.partition", "0");

        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder();
        builder.withSources(new ConfigSource() {
            @Override
            public Map<String, String> getProperties() {
                return cfg;
            }

            @Override
            public Set<String> getPropertyNames() {
                return cfg.keySet();
            }

            @Override
            public int getOrdinal() {
                return ConfigSource.DEFAULT_ORDINAL;
            }

            @Override
            public String getValue(String s) {
                return cfg.get(s);
            }

            @Override
            public String getName() {
                return "request-reply-config";
            }
        });
        SmallRyeConfig config = builder.build();

        Config outgoingConfig = Configs.outgoing(config, "kafka", "requests");
        Config replyConfig = Configs.prefixOverride(outgoingConfig, "reply",
                Map.of(
                        "topic", c -> c.getOriginalValue("topic", String.class).orElse("requests") + "-replies",
                        "assign-seek", c -> c.getOriginalValue("reply.partition", Integer.class).map(String::valueOf)
                                .orElse(null)));

        assertThat(replyConfig.getValue("topic", String.class)).isEqualTo("request-topic-replies");
        assertThat(replyConfig.getValue("assign-seek", String.class)).isEqualTo("0");
        assertThat(replyConfig.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
    }

    @Test
    public void testPropertyNamesAcrossLayers() {
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");

        Map<String, Object> fallbacks = new HashMap<>();
        fallbacks.put("fallback.key", "value");

        Config fallbackConfig = Configs.fallback(connectorConfig, fallbacks);

        Map<String, Object> overrides = new HashMap<>();
        overrides.put("override.key", "value");

        Config overrideConfig = Configs.override(fallbackConfig, overrides);
        Config prefixedConfig = Configs.prefix("dead-letter-queue", overrideConfig);

        Iterable<String> propertyNames = prefixedConfig.getPropertyNames();

        // Should include properties from all layers
        assertThat(propertyNames)
                .contains("topic", "compression.type") // DLQ-specific
                .contains("bootstrap.servers", "group.id", "acks", "linger.ms") // Main channel
                .contains("fallback.key") // Fallback
                .contains("override.key"); // Override
    }

    @Test
    public void testConfigsUtilityMethods() {
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", baseConfig, "kafka", "my-channel");

        // Test Configs.fallback
        Config fallbackConfig = Configs.fallback(connectorConfig, Map.of("key1", "value1"), Map.of("key2", "value2"));
        assertThat(fallbackConfig.getValue("key1", String.class)).isEqualTo("value1");
        assertThat(fallbackConfig.getValue("key2", String.class)).isEqualTo("value2");

        // Test Configs.override
        Config overrideConfig = Configs.override(connectorConfig, Map.of("topic", "overridden-topic"));
        assertThat(overrideConfig.getValue("topic", String.class)).isEqualTo("overridden-topic");

        // Test Configs.prefix
        Config prefixConfig = Configs.prefix("dead-letter-queue", connectorConfig);
        assertThat(prefixConfig.getValue("topic", String.class)).isEqualTo("my-dlq-topic");

        // Test Configs.prefixOverride
        Config prefixOverrideConfig = Configs.prefixOverride(connectorConfig, "dead-letter-queue",
                Map.of("acks", c -> "all"));
        assertThat(prefixOverrideConfig.getValue("topic", String.class)).isEqualTo("my-dlq-topic");
        assertThat(prefixOverrideConfig.getValue("acks", String.class)).isEqualTo("all");
    }
}
