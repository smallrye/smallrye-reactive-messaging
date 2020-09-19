package io.smallrye.reactive.messaging.kafka.impl;

import java.util.HashMap;
import java.util.Map;

import io.smallrye.reactive.messaging.kafka.KafkaConnectorCommonConfiguration;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.admin.KafkaAdminClient;

public class KafkaAdminHelper {

    private KafkaAdminHelper() {
        // avoid direct instantiation
    }

    public static KafkaAdminClient createAdminClient(KafkaConnectorCommonConfiguration configuration, Vertx vertx,
            Map<String, Object> kafkaConfigurationMap) {
        if (configuration.getHealthEnabled()) {
            Map<String, String> copy = new HashMap<>();
            for (Map.Entry<String, Object> entry : kafkaConfigurationMap.entrySet()) {
                copy.put(entry.getKey(), entry.getValue().toString());
            }
            Map<String, String> adminConfiguration = new HashMap<>(copy);
            adminConfiguration.remove("key.serializer");
            adminConfiguration.remove("value.serializer");
            adminConfiguration.remove("key.deserializer");
            adminConfiguration.remove("value.deserializer");
            adminConfiguration.remove("acks");
            adminConfiguration.remove("max.in.flight.requests.per.connection");
            adminConfiguration.remove("group.id");
            return KafkaAdminClient.create(vertx, adminConfiguration);
        } else {
            return null;
        }
    }
}
