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
            Map<String, Object> kafkaConfigurationMap, String channel) {
        Map<String, String> copy = new HashMap<>();
        for (Map.Entry<String, Object> entry : kafkaConfigurationMap.entrySet()) {
            if (AdminClientConfig.configNames().contains(entry.getKey())) {
                copy.put(entry.getKey(), entry.getValue().toString());
            }
        }
        copy.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-admin-" + channel);
        return KafkaAdminClient.create(vertx, copy);
    }
}
