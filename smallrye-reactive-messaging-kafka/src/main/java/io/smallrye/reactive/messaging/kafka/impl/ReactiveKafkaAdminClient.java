package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;

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
        return listTopics(new ListTopicsOptions());
    }

    @Override
    @CheckReturnValue
    public Uni<Set<String>> listTopics(ListTopicsOptions options) {
        return Uni.createFrom().completionStage(
                adminClient.listTopics(options).names()
                        .toCompletionStage());
    }

    @Override
    @CheckReturnValue
    public Uni<Map<String, TopicDescription>> describeTopics(Collection<String> topicNames) {
        return describeTopics(topicNames, new DescribeTopicsOptions());
    }

    @Override
    @CheckReturnValue
    public Uni<Map<String, TopicDescription>> describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        return Uni.createFrom().completionStage(
                adminClient.describeTopics(topicNames, options)
                        .allTopicNames()
                        .toCompletionStage());
    }

    @Override
    @CheckReturnValue
    public Uni<Collection<Node>> describeCluster() {
        return describeCluster(new DescribeClusterOptions());
    }

    @Override
    @CheckReturnValue
    public Uni<Collection<Node>> describeCluster(DescribeClusterOptions options) {
        return Uni.createFrom().completionStage(
                adminClient.describeCluster(options)
                        .nodes()
                        .toCompletionStage());
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
