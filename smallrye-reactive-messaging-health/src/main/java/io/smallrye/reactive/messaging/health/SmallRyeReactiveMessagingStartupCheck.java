package io.smallrye.reactive.messaging.health;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Startup;

import io.smallrye.reactive.messaging.extension.HealthCenter;

@ApplicationScoped
@Startup
public class SmallRyeReactiveMessagingStartupCheck implements HealthCheck {

    @Inject
    HealthCenter health;

    @Override
    public HealthCheckResponse call() {
        HealthReport report = health.getStartup();
        return HealthChecks.getHealthCheck(report, "startup check");
    }
}
