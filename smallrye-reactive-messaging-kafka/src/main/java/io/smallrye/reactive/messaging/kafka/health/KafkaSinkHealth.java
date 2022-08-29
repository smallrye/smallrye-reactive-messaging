package io.smallrye.reactive.messaging.kafka.health;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaAdmin;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminHelper;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaProducer;

public class KafkaSinkHealth extends BaseHealth {

    private final KafkaAdmin admin;
    private final String topic;
    private final ReactiveKafkaProducer<?, ?> client;
    private final Duration adminClientTimeout;
    private Metric metric;

    public KafkaSinkHealth(KafkaConnectorOutgoingConfiguration config,
            Map<String, ?> kafkaConfiguration, ReactiveKafkaProducer<?, ?> client) {
        super(config.getChannel());
        this.topic = config.getTopic().orElse(config.getChannel());
        this.adminClientTimeout = Duration.ofMillis(
                config.getHealthReadinessTimeout().orElse(config.getHealthTopicVerificationTimeout()));
        this.client = client;

        if (config.getHealthReadinessTopicVerification().orElse(config.getHealthTopicVerificationEnabled())) {
            // Do not create the client if the readiness health checks are disabled
            Map<String, Object> adminConfiguration = new HashMap<>(kafkaConfiguration);
            this.admin = KafkaAdminHelper.createAdminClient(adminConfiguration, config.getChannel(), true);
        } else {
            this.admin = null;
        }
    }

    protected synchronized Metric getMetric() {
        if (this.metric == null) {
            Producer<?, ?> producer = this.client.unwrap();
            if (producer != null) {
                this.metric = getMetric(producer.metrics());
            }
        }
        return this.metric;
    }

    @Override
    public KafkaAdmin getAdmin() {
        return admin;
    }

    @Override
    protected void metricsBasedStartupCheck(HealthReport.HealthReportBuilder builder) {
        Metric metric = getMetric();
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
                    .await().atMost(adminClientTimeout);
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
                    .await().atMost(adminClientTimeout);
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
