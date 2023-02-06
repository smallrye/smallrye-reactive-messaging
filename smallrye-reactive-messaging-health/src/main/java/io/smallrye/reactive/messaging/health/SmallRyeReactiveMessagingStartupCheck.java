package io.smallrye.reactive.messaging.health;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Startup;

import io.smallrye.reactive.messaging.providers.extension.HealthCenter;

@ApplicationScoped
@Startup
public class SmallRyeReactiveMessagingStartupCheck implements HealthCheck {

    @Inject
    HealthCenter health;

    @Override
    public HealthCheckResponse call() {
        if (!health.isInitialized()) {
            return HealthChecks.NOT_YET_INITIALIZED;
        }

        HealthReport report = health.getStartup();
        return HealthChecks.getHealthCheck(report, "startup check");
    }
}
