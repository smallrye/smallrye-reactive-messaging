package io.smallrye.reactive.messaging.kafka.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;

import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.admin.KafkaAdminClient;

public class KafkaAdminHelper {

    private KafkaAdminHelper() {
        // avoid direct instantiation
    }

    public static KafkaAdminClient createAdminClient(Vertx vertx,
            Map<String, ?> kafkaConfigurationMap, String channel, boolean incoming) {
        Map<String, String> copy = new HashMap<>();
        for (Map.Entry<String, ?> entry : kafkaConfigurationMap.entrySet()) {
            if (AdminClientConfig.configNames().contains(entry.getKey())) {
                copy.put(entry.getKey(), entry.getValue().toString());
            } else if (entry.getKey().startsWith("sasl.")) {
                copy.put(entry.getKey(), entry.getValue().toString());
            }
        }
        copy.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-admin-" + (incoming ? "incoming-" : "outgoing-") + channel);

        if (!kafkaConfigurationMap.containsKey(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // If no backoff is set, use 10s, it avoids high load on disconnection.
            copy.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }

        return KafkaAdminClient.create(vertx, copy);
    }
}
