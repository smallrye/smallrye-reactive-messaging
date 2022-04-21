package io.smallrye.reactive.messaging.providers.extension;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;

/**
 * Component responsible to compute the current state of the reactive messaging application.
 */
@ApplicationScoped
public class HealthCenter {

    @Inject
    @Any
    Instance<HealthReporter> reporters;

    List<ReportedFailure> failures = new CopyOnWriteArrayList<>();

    private volatile boolean initialized = false;

    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (HealthReporter r : reporters) {
            for (HealthReport.ChannelInfo channelInfo : r.getReadiness().getChannels()) {
                builder.add(channelInfo);
            }
        }
        for (ReportedFailure rf : failures) {
            builder.add(rf.source, false, rf.failure.getMessage());
        }
        return builder.build();
    }

    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (HealthReporter r : reporters) {
            for (HealthReport.ChannelInfo channelInfo : r.getLiveness().getChannels()) {
                builder.add(channelInfo);
            }
        }
        for (ReportedFailure rf : failures) {
            builder.add(rf.source, false, rf.failure.getMessage());
        }
        return builder.build();
    }

    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (HealthReporter r : reporters) {
            for (HealthReport.ChannelInfo channelInfo : r.getStartup().getChannels()) {
                builder.add(channelInfo);
            }
        }
        // failures do not contribute to the startup probe when app is running
        return builder.build();
    }

    public void report(String source, Throwable cause) {
        failures.add(new ReportedFailure(source, cause));
    }

    public void reportApplicationFailure(String method, Throwable cause) {
        failures.add(new ReportedFailure("application-" + method, cause));
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void markInitialized() {
        this.initialized = true;
    }

    public static class ReportedFailure {
        final String source;
        final Throwable failure;

        public ReportedFailure(String source, Throwable failure) {
            this.source = source;
            this.failure = failure;
        }
    }

}
