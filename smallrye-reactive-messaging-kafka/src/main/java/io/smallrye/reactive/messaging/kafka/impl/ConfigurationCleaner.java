package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.JsonObject;

public class ConfigurationCleaner {

    private static final List<String> COMMON = Arrays.asList(
            "connector",
            "channel-name",
            "topic",
            "enabled",

            "health-enabled",
            "health-readiness-enabled",
            "health-readiness-timeout",

            "tracing-enabled",
            "cloud-events");

    private static final List<String> PRODUCER = Arrays.asList(
            "key",
            "partition",
            "waitforwritecompletion", // lower case on purpose
            "max-inflight-messages",
            "cloud-events-source",
            "cloud-events-type",
            "cloud-events-subject",
            "cloud-events-data-content-type",
            "cloud-events-data-schema",
            "cloud-events-insert-timestamp",
            "cloud-events-mode",

            // Remove most common attributes, may have been configured from the default config
            "key.deserializer",
            "value.deserializer",
            "enable.auto.commit",
            "group.id",
            "auto.offset.reset");

    private static final List<String> CONSUMER = Arrays.asList(
            "topics",
            "pattern",
            "retry",
            "retry-attempts",
            "retry-max-wait",
            "broadcast",
            "failure-strategy",
            "commit-strategy",
            "throttled.unprocessed-record-max-age.ms",
            "dead-letter-queue.topic",
            "dead-letter-queue.key.serializer",
            "dead-letter-queue.value.serializer",
            "partitions",
            "consumer-rebalance-listener.name",
            "key-deserialization-failure-handler",
            "value-deserialization-failure-handler",

            // Remove most common attributes, may have been configured from the default config
            "key.serializer",
            "value.serializer");

    private ConfigurationCleaner() {
        // Avoid direct instantiation
    }

    public static JsonObject cleanupProducerConfiguration(JsonObject json) {
        for (String key : COMMON) {
            json.remove(key);
        }
        for (String key : PRODUCER) {
            json.remove(key);
        }
        return json;
    }

    public static void cleanupProducerConfiguration(Map<String, String> json) {
        for (String key : COMMON) {
            json.remove(key);
        }
        for (String key : PRODUCER) {
            json.remove(key);
        }
    }

    public static void cleanupConsumerConfiguration(Map<String, String> conf) {
        for (String key : COMMON) {
            conf.remove(key);
        }
        for (String key : CONSUMER) {
            conf.remove(key);
        }
    }

    public static Map<String, Object> asKafkaConfiguration(Map<String, String> conf) {
        return new HashMap<>(conf);
    }

}
