package io.smallrye.reactive.messaging.kafka.health;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaAdmin;

public abstract class BaseHealth {

    public static final String CONNECTION_COUNT_METRIC_NAME = "connection-count";

    protected final String channel;

    private final boolean topicVerificationEnabled;
    protected final boolean startupTopicVerificationDisabled;

    protected final boolean readinessTopicVerificationDisabled;

    public BaseHealth(String channel, boolean topicVerificationEnabled,
            boolean startupTopicVerificationDisabled, boolean readinessTopicVerificationDisabled) {
        this.channel = channel;
        this.topicVerificationEnabled = topicVerificationEnabled;
        this.startupTopicVerificationDisabled = startupTopicVerificationDisabled;
        this.readinessTopicVerificationDisabled = readinessTopicVerificationDisabled;
    }

    public boolean isReadinessTopicVerificationEnabled() {
        return topicVerificationEnabled && !readinessTopicVerificationDisabled;
    }

    public boolean isStartupTopicVerificationEnabled() {
        return topicVerificationEnabled && !startupTopicVerificationDisabled;
    }

    public void close() {
        KafkaAdmin admin = getAdmin();
        if (admin != null) {
            try {
                admin.closeAndAwait();
            } catch (Throwable e) {
                log.exceptionOnClose(e);
            }
        }
    }

    public void isStarted(HealthReport.HealthReportBuilder builder) {
        if (isStartupTopicVerificationEnabled()) {
            clientBasedStartupCheck(builder);
        } else {
            metricsBasedStartupCheck(builder);
        }
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        if (isReadinessTopicVerificationEnabled()) {
            clientBasedReadinessCheck(builder);
        } else {
            metricsBasedReadinessCheck(builder);
        }
    }

    public Metric getMetric(Map<MetricName, ? extends Metric> metrics) {
        Metric metric = null;
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals(CONNECTION_COUNT_METRIC_NAME)) {
                metric = metrics.get(metricName);
                break;
            }
        }
        return metric;
    }

    protected abstract void metricsBasedStartupCheck(HealthReport.HealthReportBuilder builder);

    protected abstract void metricsBasedReadinessCheck(HealthReport.HealthReportBuilder builder);

    protected abstract void clientBasedStartupCheck(HealthReport.HealthReportBuilder builder);

    protected abstract void clientBasedReadinessCheck(HealthReport.HealthReportBuilder builder);

    public abstract KafkaAdmin getAdmin();
}
