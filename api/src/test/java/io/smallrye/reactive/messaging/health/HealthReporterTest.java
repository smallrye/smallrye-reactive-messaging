package io.smallrye.reactive.messaging.health;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class HealthReporterTest {

    @Test
    public void testDefaults() {
        HealthReporter reporter = new HealthReporter() {
            // use default implementation.
        };

        assertThat(reporter.getLiveness().isOk()).isTrue();
        assertThat(reporter.getReadiness().isOk()).isTrue();
        assertThat(reporter.getReadiness().getChannels()).isEmpty();
        assertThat(reporter.getLiveness().getChannels()).isEmpty();
    }
}
