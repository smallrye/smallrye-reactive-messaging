package io.smallrye.reactive.messaging.health;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

import io.smallrye.reactive.messaging.providers.extension.HealthCenter;

@ApplicationScoped
@Readiness
public class SmallRyeReactiveMessagingReadinessCheck implements HealthCheck {

    @Inject
    HealthCenter health;

    @Override
    public HealthCheckResponse call() {
        if (!health.isInitialized()) {
            return HealthChecks.NOT_YET_INITIALIZED;
        }

        HealthReport report = health.getReadiness();
        return HealthChecks.getHealthCheck(report, "readiness check");
    }
}
