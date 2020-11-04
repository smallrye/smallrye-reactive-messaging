package io.smallrye.reactive.messaging.health;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.health.*;

import io.smallrye.reactive.messaging.extension.HealthCenter;

@ApplicationScoped
@Liveness
public class SmallRyeReactiveMessagingLivenessCheck implements HealthCheck {

    @Inject
    HealthCenter health;

    @Override
    public HealthCheckResponse call() {
        HealthReport report = health.getLiveness();
        return HealthChecks.getHealthCheck(report, "liveness check");
    }

}
