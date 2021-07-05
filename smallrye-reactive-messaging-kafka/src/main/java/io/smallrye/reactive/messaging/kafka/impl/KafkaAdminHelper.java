package io.smallrye.reactive.messaging.kafka.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.smallrye.reactive.messaging.kafka.KafkaAdmin;

public class KafkaAdminHelper {

    private KafkaAdminHelper() {
        // avoid direct instantiation
    }

    public static KafkaAdmin createAdminClient(Map<String, Object> kafkaConfigurationMap, String channel,
            boolean incoming) {
        Map<String, String> copy = new HashMap<>();
        for (Map.Entry<String, Object> entry : kafkaConfigurationMap.entrySet()) {
            if (AdminClientConfig.configNames().contains(entry.getKey())) {
                copy.put(entry.getKey(), entry.getValue().toString());
            } else if (entry.getKey().startsWith("sasl.")) {
                copy.put(entry.getKey(), entry.getValue().toString());
            }
        }

        if (!kafkaConfigurationMap.containsKey(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // If no backoff is set, use 10s, it avoids high load on disconnection.
            copy.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }

        String name;
        Object id = kafkaConfigurationMap.get(ConsumerConfig.CLIENT_ID_CONFIG);
        if (id != null) {
            name = "kafka-admin-" + id + "-" + channel;
        } else {
            name = "kafka-admin-" + (incoming ? "incoming-" : "outgoing-") + channel;
        }
        copy.put(AdminClientConfig.CLIENT_ID_CONFIG, name);

        return new ReactiveKafkaAdminClient(copy);
    }
}
