package io.smallrye.reactive.messaging.kafka.health;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminHelper;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.admin.KafkaAdminClient;

public class KafkaSourceReadinessHealth {

    public static final String CONNECTION_COUNT_METRIC_NAME = "connection-count";
    private final KafkaAdminClient admin;
    private final KafkaConnectorIncomingConfiguration config;
    private final Pattern pattern;
    private final String channel;
    private final Set<String> topics;
    private final Metric metric;

    public KafkaSourceReadinessHealth(Vertx vertx, KafkaConnectorIncomingConfiguration config,
            Map<String, String> kafkaConfiguration, Consumer<?, ?> consumer, Set<String> topics, Pattern pattern) {
        this.config = config;
        this.channel = config.getChannel();
        this.topics = topics;
        this.pattern = pattern;

        if (config.getHealthReadinessTopicVerification()) {
            // Do not create the client if the readiness health checks are disabled
            Map<String, Object> adminConfiguration = new HashMap<>(kafkaConfiguration);
            this.admin = KafkaAdminHelper.createAdminClient(vertx, adminConfiguration, config.getChannel(), true);
            this.metric = null;
        } else {
            this.admin = null;
            Map<MetricName, ? extends Metric> metrics = consumer.metrics();
            Metric metric = null;
            for (MetricName metricName : metrics.keySet()) {
                if (metricName.name().equals(CONNECTION_COUNT_METRIC_NAME)) {
                    metric = metrics.get(metricName);
                    break;
                }
            }
            this.metric = metric;
        }
    }

    public void close() {
        if (admin != null) {
            try {
                this.admin.closeAndAwait();
            } catch (Throwable e) {
                log.exceptionOnClose(e);
            }
        }
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        if (admin != null) {
            adminBasedHealthCheck(builder);
        } else {
            metricsBasedHealthCheck(builder);
        }
    }

    private void metricsBasedHealthCheck(HealthReport.HealthReportBuilder builder) {
        if (metric != null) {
            builder.add(channel, (double) metric.metricValue() >= 1.0);
        } else {
            builder.add(channel, true).build();
        }
    }

    protected void adminBasedHealthCheck(HealthReport.HealthReportBuilder builder) {
        Set<String> existingTopics;
        try {
            existingTopics = admin.listTopics()
                    .await().atMost(Duration.ofMillis(config.getHealthReadinessTimeout()));
            if (pattern == null && existingTopics.containsAll(topics)) {
                builder.add(channel, true);
            } else if (pattern != null) {
                // Check that at least one topic matches
                boolean ok = existingTopics.stream()
                        .anyMatch(s -> pattern.matcher(s).matches());
                if (ok) {
                    builder.add(channel, ok);
                } else {
                    builder.add(channel, false,
                            "Unable to find a topic matching the given pattern: " + pattern);
                }
            } else {
                String missing = topics.stream().filter(s -> !existingTopics.contains(s))
                        .collect(Collectors.joining());
                builder.add(channel, false, "Unable to find topic(s): " + missing);
            }
        } catch (Exception failed) {
            builder.add(channel, false, "No response from broker for channel "
                    + channel + " : " + failed);
        }
    }
}
