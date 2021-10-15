package io.smallrye.reactive.messaging.kafka.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaAdmin;

public class ReactiveKafkaAdminClient implements KafkaAdmin {

    private final AdminClient adminClient;

    public ReactiveKafkaAdminClient(Map<String, String> config) {
        adminClient = AdminClient.create(new HashMap<>(config));
    }

    @Override
    @CheckReturnValue
    public Uni<Set<String>> listTopics() {
        return Uni.createFrom().future(adminClient.listTopics().names());
    }

    @Override
    public Admin unwrap() {
        return adminClient;
    }

    @Override
    public void closeAndAwait() {
        adminClient.close();
    }
}
