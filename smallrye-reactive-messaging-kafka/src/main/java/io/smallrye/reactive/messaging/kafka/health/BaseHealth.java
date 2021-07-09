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

    public BaseHealth(String channel) {
        this.channel = channel;
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

    public void isReady(HealthReport.HealthReportBuilder builder) {
        KafkaAdmin admin = getAdmin();
        if (admin != null) {
            adminBasedHealthCheck(builder);
        } else {
            metricsBasedHealthCheck(builder);
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

    protected abstract void metricsBasedHealthCheck(HealthReport.HealthReportBuilder builder);

    protected abstract void adminBasedHealthCheck(HealthReport.HealthReportBuilder builder);

    public abstract KafkaAdmin getAdmin();
}
