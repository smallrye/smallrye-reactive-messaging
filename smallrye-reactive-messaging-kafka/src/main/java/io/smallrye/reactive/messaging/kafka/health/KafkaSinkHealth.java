package io.smallrye.reactive.messaging.kafka.health;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaAdmin;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminHelper;

public class KafkaSinkHealth extends BaseHealth {

    private final KafkaConnectorOutgoingConfiguration config;
    private final KafkaAdmin admin;
    private final Metric metric;
    private final String topic;

    public KafkaSinkHealth(KafkaConnectorOutgoingConfiguration config,
            Map<String, ?> kafkaConfiguration, Producer<?, ?> producer) {
        super(config.getChannel());
        this.topic = config.getTopic().orElse(config.getChannel());
        this.config = config;

        if (config.getHealthReadinessTopicVerification()) {
            // Do not create the client if the readiness health checks are disabled
            Map<String, Object> adminConfiguration = new HashMap<>(kafkaConfiguration);
            this.admin = KafkaAdminHelper.createAdminClient(adminConfiguration, config.getChannel(), true);
            this.metric = null;
        } else {
            this.admin = null;
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            this.metric = getMetric(metrics);
        }
    }

    @Override
    public KafkaAdmin getAdmin() {
        return admin;
    }

    @Override
    protected void metricsBasedStartupCheck(HealthReport.HealthReportBuilder builder) {
        if (metric != null) {
            builder.add(channel, (double) metric.metricValue() >= 1.0);
        } else {
            builder.add(channel, true).build();
        }
    }

    protected void metricsBasedReadinessCheck(HealthReport.HealthReportBuilder builder) {
        metricsBasedStartupCheck(builder);
    }

    @Override
    protected void clientBasedStartupCheck(HealthReport.HealthReportBuilder builder) {
        try {
            admin.listTopics()
                    .await().atMost(Duration.ofMillis(config.getHealthReadinessTimeout()));
            builder.add(channel, true);
        } catch (Exception failed) {
            builder.add(channel, false, "Failed to get response from broker for channel "
                    + channel + " : " + failed);
        }
    }

    protected void clientBasedReadinessCheck(HealthReport.HealthReportBuilder builder) {
        Set<String> topics;
        try {
            topics = admin.listTopics()
                    .await().atMost(Duration.ofMillis(config.getHealthReadinessTimeout()));
            if (topics.contains(topic)) {
                builder.add(channel, true);
            } else {
                builder.add(channel, false, "Unable to find topic " + topic);
            }
        } catch (Exception failed) {
            builder.add(channel, false, "No response from broker for topic "
                    + topic + " : " + failed);
        }
    }
}
