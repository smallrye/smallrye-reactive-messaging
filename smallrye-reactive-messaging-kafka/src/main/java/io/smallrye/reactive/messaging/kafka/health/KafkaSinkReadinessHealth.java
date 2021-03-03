package io.smallrye.reactive.messaging.kafka.health;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminHelper;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.admin.KafkaAdminClient;

public class KafkaSinkReadinessHealth extends BaseHealth {

    private final KafkaConnectorOutgoingConfiguration config;
    private final KafkaAdminClient admin;
    private final Metric metric;
    private final String topic;

    public KafkaSinkReadinessHealth(Vertx vertx, KafkaConnectorOutgoingConfiguration config,
            Map<String, Object> kafkaConfiguration, Producer<?, ?> producer) {
        super(config.getChannel());
        this.topic = config.getTopic().orElse(config.getChannel());
        this.config = config;

        if (config.getHealthReadinessTopicVerification()) {
            // Do not create the client if the readiness health checks are disabled
            this.admin = KafkaAdminHelper.createAdminClient(vertx, kafkaConfiguration, config.getChannel(), true);
            this.metric = null;
        } else {
            this.admin = null;
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            this.metric = getMetric(metrics);
        }
    }

    @Override
    public KafkaAdminClient getAdmin() {
        return admin;
    }

    protected void metricsBasedHealthCheck(HealthReport.HealthReportBuilder builder) {
        if (metric != null) {
            builder.add(channel, (double) metric.metricValue() >= 1.0);
        } else {
            builder.add(channel, true).build();
        }
    }

    protected void adminBasedHealthCheck(HealthReport.HealthReportBuilder builder) {
        Set<String> topics;
        try {
            topics = admin.listTopics()
                    .await().atMost(Duration.ofMillis(config.getHealthReadinessTimeout()));
            if (topics.contains(topic)) {
                builder.add(config.getChannel(), true);
            } else {
                builder.add(config.getChannel(), false, "Unable to find topic " + topic);
            }
        } catch (Exception failed) {
            builder.add(config.getChannel(), false, "No response from broker for topic "
                    + topic + " : " + failed);
        }
    }
}
