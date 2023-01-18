package io.smallrye.reactive.messaging.kafka.health;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaAdmin;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminHelper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;

public class KafkaSourceHealth extends BaseHealth {

    private final KafkaAdmin admin;
    private final KafkaSource<?, ?> source;
    private final ReactiveKafkaConsumer<?, ?> client;
    private final Set<String> topics;
    private final Pattern pattern;
    private final Duration adminClientTimeout;
    private Metric metric;

    public KafkaSourceHealth(KafkaSource<?, ?> source, KafkaConnectorIncomingConfiguration config,
            ReactiveKafkaConsumer<?, ?> client, Set<String> topics, Pattern pattern) {
        super(config.getChannel(),
                config.getHealthReadinessTopicVerification().orElse(config.getHealthTopicVerificationEnabled()),
                config.getHealthTopicVerificationStartupDisabled(),
                config.getHealthTopicVerificationReadinessDisabled());
        this.adminClientTimeout = Duration.ofMillis(
                config.getHealthReadinessTimeout().orElse(config.getHealthTopicVerificationTimeout()));
        this.source = source;
        this.client = client;
        this.topics = topics;
        this.pattern = pattern;
        if (config.getHealthReadinessTopicVerification().orElse(config.getHealthTopicVerificationEnabled())) {
            // Do not create the client if the readiness health checks are disabled
            Map<String, Object> adminConfiguration = new HashMap<>(client.configuration());
            this.admin = KafkaAdminHelper.createAdminClient(adminConfiguration, config.getChannel(), true);
        } else {
            this.admin = null;
        }
    }

    protected synchronized Metric getMetric() {
        if (this.metric == null) {
            Consumer<?, ?> consumer = this.client.unwrap();
            if (consumer != null) {
                this.metric = getMetric(consumer.metrics());
            }
        }
        return this.metric;
    }

    @Override
    protected void metricsBasedStartupCheck(HealthReport.HealthReportBuilder builder) {
        Metric metric = getMetric();
        if (metric != null) {
            boolean connected = (double) metric.metricValue() >= 1.0;
            boolean hasSubscribers = source.hasSubscribers();
            if (connected) {
                builder.add(channel, true);
            } else if (!hasSubscribers) {
                builder.add(channel, true, "no subscription yet, so no connection to the Kafka broker yet");
            } else {
                builder.add(channel, false);
            }
        } else {
            builder.add(channel, true).build();
        }
    }

    @Override
    protected void metricsBasedReadinessCheck(HealthReport.HealthReportBuilder builder) {
        metricsBasedStartupCheck(builder);
    }

    @Override
    protected void clientBasedStartupCheck(HealthReport.HealthReportBuilder builder) {
        if (pattern == null) {
            checkTopicExists(builder);
        } else {
            checkTopicExistsForPattern(builder);
        }
    }

    private void checkTopicExists(HealthReport.HealthReportBuilder builder) {
        try {
            Map<String, TopicDescription> found = admin.describeTopics(topics, new DescribeTopicsOptions()
                    .includeAuthorizedOperations(false)
                    .timeoutMs((int) adminClientTimeout.toMillis()))
                    .await().atMost(adminClientTimeout);
            if (found.keySet().containsAll(topics)) {
                if (topics.stream().allMatch(t -> found.get(t).partitions().stream().allMatch(info -> info.leader() != null))) {
                    builder.add(channel, true);
                } else {
                    builder.add(channel, false, "Unable to find leaders for all partitions of topics " + topics);
                }
            } else {
                builder.add(channel, false, "Unable to find topic(s) " + topics + " in " + found.keySet());
            }
        } catch (Exception failed) {
            builder.add(channel, false, "No response from broker for topics " + topics + " : " + failed);
        }
    }

    private void checkTopicExistsForPattern(HealthReport.HealthReportBuilder builder) {
        try {
            Set<String> found = admin.listTopics(new ListTopicsOptions()
                    .timeoutMs((int) adminClientTimeout.toMillis()))
                    .await().atMost(adminClientTimeout);
            if (found.stream().anyMatch(t -> pattern.matcher(t).matches())) {
                builder.add(channel, true);
            } else {
                builder.add(channel, false, "Unable to find topic(s) matching pattern " + pattern + " in " + found);
            }
        } catch (Exception failed) {
            builder.add(channel, false, "No response from broker for topic(s) " + pattern + " : " + failed);
        }
    }

    @Override
    protected void clientBasedReadinessCheck(HealthReport.HealthReportBuilder builder) {
        if (source.hasSubscribers()) {
            Set<TopicPartition> partitions;
            try {
                partitions = client.getAssignments()
                        .await().atMost(adminClientTimeout);
                if (partitions.isEmpty()) {
                    builder.add(channel, false, "No partition assignments for channel " + channel);
                } else {
                    builder.add(channel, true);
                }
            } catch (Exception failed) {
                builder.add(channel, false, "No response from broker for channel "
                        + channel + " : " + failed);
            }
        } else {
            builder.add(channel, true, "no subscription yet, so no partition assignments");
        }
    }

    @Override
    public KafkaAdmin getAdmin() {
        return admin;
    }
}
