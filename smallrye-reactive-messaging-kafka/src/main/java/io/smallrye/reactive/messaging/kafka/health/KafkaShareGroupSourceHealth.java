package io.smallrye.reactive.messaging.kafka.health;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaAdmin;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminClientRegistry;
import io.smallrye.reactive.messaging.kafka.impl.KafkaShareGroupSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaShareConsumer;

public class KafkaShareGroupSourceHealth extends BaseHealth {

    private final KafkaAdmin admin;
    private final KafkaShareGroupSource<?, ?> source;
    private final ReactiveKafkaShareConsumer<?, ?> client;
    private final Set<String> topics;
    private final Duration adminClientTimeout;

    public KafkaShareGroupSourceHealth(KafkaShareGroupSource<?, ?> source, KafkaConnectorIncomingConfiguration config,
            ReactiveKafkaShareConsumer<?, ?> client, KafkaAdminClientRegistry adminClientRegistry, Set<String> topics) {
        super(config.getChannel(),
                adminClientRegistry,
                config.getHealthReadinessTopicVerification().orElse(config.getHealthTopicVerificationEnabled()),
                config.getHealthTopicVerificationStartupDisabled(),
                config.getHealthTopicVerificationReadinessDisabled());
        this.adminClientTimeout = Duration.ofMillis(
                config.getHealthReadinessTimeout().orElse(config.getHealthTopicVerificationTimeout()));
        this.source = source;
        this.client = client;
        this.topics = topics;
        if (config.getHealthReadinessTopicVerification().orElse(config.getHealthTopicVerificationEnabled())) {
            // Do not create the client if the readiness health checks are disabled
            Map<String, Object> adminConfiguration = new HashMap<>(client.configuration());
            this.admin = adminClientRegistry.getOrCreateAdminClient(adminConfiguration, config.getChannel(), true);
        } else {
            this.admin = null;
        }
    }

    @Override
    protected Optional<Map<MetricName, ? extends Metric>> getMetrics() {
        return Optional.ofNullable(this.client.unwrap())
                .map(ShareConsumer::metrics);
    }

    @Override
    protected void metricsBasedStartupCheck(HealthReport.HealthReportBuilder builder) {
        Metric connectionCountMetric = getConnectionCountMetric();
        Metric connectionCreationTotalMetric = getConnectionCreationTotalMetric();

        if (connectionCountMetric != null && connectionCreationTotalMetric != null) {
            boolean connected = (double) connectionCountMetric.metricValue() >= 1.0;
            boolean hasSubscribers = source.hasSubscribers();
            boolean connectionAttempted = (double) connectionCreationTotalMetric.metricValue() > 0;
            if (connected) {
                builder.add(channel, true);
            } else if (!hasSubscribers) {
                builder.add(channel, true, "no subscription yet, so no connection to the Kafka broker yet");
            } else if (!connectionAttempted) {
                builder.add(channel, true, "no connection was ever attempted, the channel was started as paused");
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
        checkTopicExists(builder);
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

    @Override
    protected void clientBasedReadinessCheck(HealthReport.HealthReportBuilder builder) {
        if (source.hasSubscribers()) {
            try {
                String shareGroup = client.getShareGroup();
                String clientId = client.getClientId();
                Map<String, ShareGroupDescription> groupDescriptions = admin.describeShareGroup(Set.of(shareGroup))
                        .await().atMost(adminClientTimeout);
                if (groupDescriptions != null) {
                    ShareGroupDescription description = groupDescriptions.get(shareGroup);
                    if (description != null) {
                        ShareMemberDescription member = description.members().stream()
                                .filter(m -> m.clientId().equals(clientId))
                                .findFirst().orElse(null);
                        if (member != null) {
                            if (!member.assignment().topicPartitions().isEmpty()) {
                                builder.add(channel, true);
                                return;
                            }
                        }
                    }
                }
                builder.add(channel, false, "No partition assignments for channel " + channel);
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
