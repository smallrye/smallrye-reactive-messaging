package io.smallrye.reactive.messaging.extension;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

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

    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        reporters.forEach(r -> r.getReadiness().getChannels().forEach(builder::add));
        failures.forEach(rf -> builder.add(rf.source, false, rf.failure.getMessage()));
        return builder.build();
    }

    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        reporters.forEach(r -> r.getLiveness().getChannels().forEach(builder::add));
        failures.forEach(rf -> builder.add(rf.source, false, rf.failure.getMessage()));
        return builder.build();
    }

    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        reporters.forEach(r -> r.getStartup().getChannels().forEach(builder::add));
        // failures do not contribute to the startup probe when app is running
        return builder.build();
    }

    public void report(String source, Throwable cause) {
        failures.add(new ReportedFailure(source, cause));
    }

    public void reportApplicationFailure(String method, Throwable cause) {
        failures.add(new ReportedFailure("application-" + method, cause));
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
