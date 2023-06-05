package io.smallrye.reactive.messaging.health;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.health.*;

import io.smallrye.reactive.messaging.providers.extension.ObservationCenter;

@ApplicationScoped
@Liveness
public class SmallRyeReactiveMessagingLivenessCheck implements HealthCheck {

    @Inject
    ObservationCenter health;

    @Override
    public HealthCheckResponse call() {
        if (!health.isInitialized()) {
            return HealthChecks.NOT_YET_INITIALIZED;
        }

        HealthReport report = health.getLiveness();
        return HealthChecks.getHealthCheck(report, "liveness check");
    }

}
