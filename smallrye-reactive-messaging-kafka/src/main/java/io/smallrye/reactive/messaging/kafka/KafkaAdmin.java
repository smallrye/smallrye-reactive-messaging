package io.smallrye.reactive.messaging.kafka;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;

import io.smallrye.mutiny.Uni;

/**
 * Internal interface for Kafka admin client.
 * To complete with remaining {@link org.apache.kafka.clients.admin.Admin} method wrappers, if decided to expose it externally.
 */
public interface KafkaAdmin {

    Uni<Set<String>> listTopics();

    Uni<Set<String>> listTopics(ListTopicsOptions options);

    Uni<Map<String, TopicDescription>> describeTopics(Collection<String> topicNames);

    Uni<Map<String, TopicDescription>> describeTopics(Collection<String> topicNames, DescribeTopicsOptions options);

    Uni<Collection<Node>> describeCluster();

    Uni<Collection<Node>> describeCluster(DescribeClusterOptions options);

    Admin unwrap();

    void closeAndAwait();
}
