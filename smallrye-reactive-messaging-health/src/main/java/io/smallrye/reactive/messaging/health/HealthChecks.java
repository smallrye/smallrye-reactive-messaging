package io.smallrye.reactive.messaging.health;

import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;

public class HealthChecks {
    public static final HealthCheckResponse NOT_YET_INITIALIZED = HealthCheckResponse.builder()
            .name("SmallRye Reactive Messaging - not yet initialized")
            .down()
            .build();

    public static HealthCheckResponse getHealthCheck(HealthReport report, String check) {
        HealthCheckResponseBuilder builder = HealthCheckResponse.builder()
                .name("SmallRye Reactive Messaging - " + check)
                .status(report.isOk());

        report.getChannels().forEach(ci -> {
            String msg = "";
            if (ci.getMessage() != null) {
                msg = " - " + ci.getMessage();
            }
            if (ci.isOk()) {
                msg = "[OK]" + msg;
            } else {
                msg = "[KO]" + msg;
            }
            builder.withData(ci.getChannel(), msg);
        });

        return builder.build();
    }
}
