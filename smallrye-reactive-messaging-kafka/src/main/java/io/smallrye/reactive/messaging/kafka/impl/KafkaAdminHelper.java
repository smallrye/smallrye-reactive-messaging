package io.smallrye.reactive.messaging.kafka.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.admin.KafkaAdminClient;

public class KafkaAdminHelper {

    private KafkaAdminHelper() {
        // avoid direct instantiation
    }

    public static KafkaAdminClient createAdminClient(Vertx vertx,
            Map<String, Object> kafkaConfigurationMap, String channel, boolean incoming) {
        Map<String, String> copy = new HashMap<>();
        for (Map.Entry<String, Object> entry : kafkaConfigurationMap.entrySet()) {
            if (AdminClientConfig.configNames().contains(entry.getKey())) {
                copy.put(entry.getKey(), entry.getValue().toString());
            }
        }

        String name;
        Object id = kafkaConfigurationMap.get(ConsumerConfig.CLIENT_ID_CONFIG);
        if (id != null) {
            name = "kafka-admin-" + id + "-" + channel;
        } else {
            name = "kafka-admin-" + (incoming ? "incoming-" : "outgoing-") + channel;
        }
        copy.put(AdminClientConfig.CLIENT_ID_CONFIG, name);
        return KafkaAdminClient.create(vertx, copy);
    }
}
