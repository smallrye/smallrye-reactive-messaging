package io.smallrye.reactive.messaging.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Startup;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.providers.extension.HealthCenter;

public class SmallRyeReactiveMessagingStartupCheckTest {

    @Test
    public void testWithOneConnector() {
        SeContainerInitializer initializer = SeContainerInitializer.newInstance().disableDiscovery();
        initializer.addBeanClasses(HealthCenter.class, MyReporterA.class,
                SmallRyeReactiveMessagingStartupCheck.class);
        SeContainer container = initializer.initialize();

        SmallRyeReactiveMessagingStartupCheck check = container.select(SmallRyeReactiveMessagingStartupCheck.class,
                Startup.Literal.INSTANCE).get();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);

        HealthCenter healthCenter = container.select(HealthCenter.class).get();
        healthCenter.markInitialized();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
        assertThat(check.call().getData().orElse(null)).containsExactly(entry("my-channel", "[OK]"));

        MyReporterA a = container.select(MyReporterA.class, ConnectorLiteral.of("connector-a")).get();

        a.toggle();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
        assertThat(check.call().getData().orElse(null)).containsExactly(entry("my-channel", "[KO]"));

        a.toggle();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
        assertThat(check.call().getData().orElse(null)).containsExactly(entry("my-channel", "[OK]"));
    }

    @Test
    public void testWithNoConnector() {
        SeContainerInitializer initializer = SeContainerInitializer.newInstance().disableDiscovery();
        initializer.addBeanClasses(HealthCenter.class, SmallRyeReactiveMessagingStartupCheck.class);
        SeContainer container = initializer.initialize();

        SmallRyeReactiveMessagingStartupCheck check = container.select(SmallRyeReactiveMessagingStartupCheck.class,
                Startup.Literal.INSTANCE).get();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);

        HealthCenter healthCenter = container.select(HealthCenter.class).get();
        healthCenter.markInitialized();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
        assertThat(check.call().getData()).isEmpty();
    }

    @ApplicationScoped
    @Connector("connector-a")
    private static class MyReporterA implements HealthReporter {

        boolean ok = true;

        @Override
        public HealthReport getStartup() {
            return HealthReport.builder().add("my-channel", ok).build();
        }

        public void toggle() {
            ok = !ok;
        }
    }

}
