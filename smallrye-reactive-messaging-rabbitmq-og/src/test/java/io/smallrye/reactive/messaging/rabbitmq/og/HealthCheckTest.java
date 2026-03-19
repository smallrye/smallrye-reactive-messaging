package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.health.HealthReport;

/**
 * Tests for health check functionality
 * Note: Health check logic is also tested as part of the channel tests.
 * This test verifies the basic health check integration at the method level.
 */
public class HealthCheckTest {

    @Test
    public void testHealthReportBuilder() {
        // Verify the health report builder works as expected
        HealthReport.HealthReportBuilder builder = HealthReport.builder();

        // Add a healthy channel
        builder.add(new HealthReport.ChannelInfo("test-channel-1", true));

        // Add an unhealthy channel
        builder.add(new HealthReport.ChannelInfo("test-channel-2", false));

        HealthReport report = builder.build();

        assertThat(report.getChannels()).hasSize(2);
        assertThat(report.getChannels().get(0).getChannel()).isEqualTo("test-channel-1");
        assertThat(report.getChannels().get(0).isOk()).isTrue();
        assertThat(report.getChannels().get(1).getChannel()).isEqualTo("test-channel-2");
        assertThat(report.getChannels().get(1).isOk()).isFalse();

        // Overall health should be unhealthy if any channel is unhealthy
        assertThat(report.isOk()).isFalse();
    }

    @Test
    public void testHealthReportAllHealthy() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        builder.add(new HealthReport.ChannelInfo("test-channel-1", true));
        builder.add(new HealthReport.ChannelInfo("test-channel-2", true));

        HealthReport report = builder.build();

        assertThat(report.getChannels()).hasSize(2);
        assertThat(report.isOk()).isTrue();
    }
}
