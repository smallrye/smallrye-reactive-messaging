package io.smallrye.reactive.messaging.kafka.health;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
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
        super(config.getChannel(),
                config.getHealthReadinessTopicVerification().orElse(config.getHealthTopicVerificationEnabled()),
                config.getHealthTopicVerificationStartupDisabled(),
                config.getHealthTopicVerificationReadinessDisabled());
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
            Map<String, TopicDescription> topics = admin
                    .describeTopics(Collections.singleton(topic), new DescribeTopicsOptions()
                            .timeoutMs((int) adminClientTimeout.toMillis())
                            .includeAuthorizedOperations(false))
                    .await().atMost(adminClientTimeout);
            if (topics.containsKey(topic)) {
                if (topics.get(topic).partitions().stream().allMatch(info -> info.leader() != null)) {
                    builder.add(channel, true);
                } else {
                    builder.add(channel, false, "Unable to find leaders for all partitions of topic " + topic);
                }
            } else {
                builder.add(channel, false, "Unable to find topic " + topic);
            }
        } catch (Exception failed) {
            builder.add(channel, false, "No response from broker for topic " + topic + " : " + failed);
        }
    }

    @Override
    protected void clientBasedReadinessCheck(HealthReport.HealthReportBuilder builder) {
        try {
            admin.describeCluster(new DescribeClusterOptions()
                    .timeoutMs((int) adminClientTimeout.toMillis())
                    .includeAuthorizedOperations(false))
                    .await().atMost(adminClientTimeout);
            builder.add(channel, true);
        } catch (Exception failed) {
            builder.add(channel, false, "Failed to get response from broker for channel "
                    + channel + " : " + failed);
        }
    }
}
