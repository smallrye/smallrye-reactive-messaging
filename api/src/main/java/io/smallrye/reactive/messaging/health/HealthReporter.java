package io.smallrye.reactive.messaging.health;

/**
 * Interface implemented by connector to participate to the health data collection.
 */
public interface HealthReporter {

    default HealthReport getReadiness() {
        return HealthReport.OK_INSTANCE;
    }

    default HealthReport getLiveness() {
        return HealthReport.OK_INSTANCE;
    }

    default HealthReport getStartup() {
        return HealthReport.OK_INSTANCE;
    }

}
