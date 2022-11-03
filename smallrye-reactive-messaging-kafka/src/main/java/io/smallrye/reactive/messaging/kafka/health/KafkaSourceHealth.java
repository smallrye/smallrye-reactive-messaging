package io.smallrye.reactive.messaging.kafka.health;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
    private final Duration adminClientTimeout;
    private Metric metric;

    public KafkaSourceHealth(KafkaSource<?, ?> source, KafkaConnectorIncomingConfiguration config,
            ReactiveKafkaConsumer<?, ?> client) {
        super(config.getChannel());
        this.adminClientTimeout = Duration.ofMillis(
                config.getHealthReadinessTimeout().orElse(config.getHealthTopicVerificationTimeout()));
        this.source = source;
        this.client = client;
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
        try {
            admin.listTopics()
                    .await().atMost(adminClientTimeout);
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
