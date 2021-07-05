package io.smallrye.reactive.messaging.kafka;

import java.util.Set;

import org.apache.kafka.clients.admin.Admin;

import io.smallrye.mutiny.Uni;

/**
 * Internal interface for Kafka admin client.
 * To complete with remaining {@link org.apache.kafka.clients.admin.Admin} method wrappers, if decided to expose it externally.
 */
public interface KafkaAdmin {

    Uni<Set<String>> listTopics();

    Admin unwrap();

    void closeAndAwait();
}
