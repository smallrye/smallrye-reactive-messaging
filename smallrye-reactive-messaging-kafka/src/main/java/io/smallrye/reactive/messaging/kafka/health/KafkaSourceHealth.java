package io.smallrye.reactive.messaging.kafka.health;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaAdmin;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminHelper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;

public class KafkaSourceHealth extends BaseHealth {

    private final KafkaAdmin admin;
    private final KafkaConnectorIncomingConfiguration config;
    private final String channel;
    private final Metric metric;
    private final KafkaSource<?, ?> source;
    private final ReactiveKafkaConsumer<?, ?> client;

    public KafkaSourceHealth(KafkaSource<?, ?> source, KafkaConnectorIncomingConfiguration config,
            ReactiveKafkaConsumer<?, ?> client) {
        super(config.getChannel());
        this.config = config;
        this.channel = config.getChannel();
        this.source = source;
        this.client = client;
        if (config.getHealthReadinessTopicVerification().orElse(config.getHealthTopicVerificationEnabled())) {
            // Do not create the client if the readiness health checks are disabled
            Map<String, Object> adminConfiguration = new HashMap<>(client.configuration());
            this.admin = KafkaAdminHelper.createAdminClient(adminConfiguration, config.getChannel(), true);
            this.metric = null;

        } else {
            this.admin = null;
            Map<MetricName, ? extends Metric> metrics = client.unwrap().metrics();
            this.metric = getMetric(metrics);
        }
    }

    @Override
    protected void metricsBasedStartupCheck(HealthReport.HealthReportBuilder builder) {
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
        try {
            long timeout = config.getHealthReadinessTimeout().orElse(config.getHealthTopicVerificationTimeout());
            admin.listTopics()
                    .await().atMost(Duration.ofMillis(timeout));
            builder.add(channel, true);
        } catch (Exception failed) {
            builder.add(channel, false, "Failed to get response from broker for channel "
                    + channel + " : " + failed);
        }
    }

    protected void clientBasedReadinessCheck(HealthReport.HealthReportBuilder builder) {
        if (source.hasSubscribers()) {
            Set<TopicPartition> partitions;
            try {
                long timeout = config.getHealthReadinessTimeout().orElse(config.getHealthTopicVerificationTimeout());
                partitions = client.getAssignments()
                        .await().atMost(Duration.ofMillis(timeout));
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
