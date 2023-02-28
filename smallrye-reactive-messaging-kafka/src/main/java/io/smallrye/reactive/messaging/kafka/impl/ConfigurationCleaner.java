package io.smallrye.reactive.messaging.kafka.impl;

import java.util.*;

public class ConfigurationCleaner {

    private static final List<String> COMMON = Arrays.asList(
            "connector",
            "channel-name",
            "topic",
            "enabled",

            "health-enabled",
            "health-readiness-enabled",
            "health-readiness-topic-verification",
            "health-readiness-timeout",
            "health-topic-verification-enabled",
            "health-topic-verification-timeout",

            "tracing-enabled",
            "cloud-events",
            "client-id-prefix",
            "lazy-client",
            "delayed-retry-topic.topics",
            "delayed-retry-topic.max-retries",
            "delayed-retry-topic.timeout");

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
            "propagate-record-key",
            "propagate-headers",
            "key-serialization-failure-handler",
            "value-serialization-failure-handler",
            "merge",
            "interceptor-bean",

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
            "batch",
            "max-queue-size-factor",
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
            "fail-on-deserialization-failure",
            "graceful-shutdown",
            "poll-timeout",
            "pause-if-no-requests",

            // Remove most common attributes, may have been configured from the default config
            "key.serializer",
            "value.serializer");

    private static final List<String> CONSUMER_PREFIX = Arrays.asList("checkpoint");

    private ConfigurationCleaner() {
        // Avoid direct instantiation
    }

    public static void cleanupProducerConfiguration(Map<String, ?> json) {
        for (String key : COMMON) {
            json.remove(key);
        }
        for (String key : PRODUCER) {
            json.remove(key);
        }
    }

    public static void cleanupConsumerConfiguration(Map<String, ?> conf) {
        for (String key : COMMON) {
            conf.remove(key);
        }
        for (String key : CONSUMER) {
            conf.remove(key);
        }
        for (String prefix : CONSUMER_PREFIX) {
            conf.keySet().removeIf(key -> key.startsWith(prefix));
        }
    }

    public static Map<String, Object> asKafkaConfiguration(Map<String, String> conf) {
        return new HashMap<>(conf);
    }

}
