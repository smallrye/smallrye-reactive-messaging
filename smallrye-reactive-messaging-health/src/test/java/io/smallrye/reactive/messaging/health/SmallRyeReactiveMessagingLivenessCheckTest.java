package io.smallrye.reactive.messaging.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.Test;

import io.smallrye.reactive.messaging.extension.HealthCenter;

public class SmallRyeReactiveMessagingLivenessCheckTest {

    @Test
    public void testWithTwoConnectors() {
        SeContainerInitializer initializer = SeContainerInitializer.newInstance().disableDiscovery();
        initializer.addBeanClasses(HealthCenter.class, MyReporterA.class, MyReporterB.class,
                SmallRyeReactiveMessagingLivenessCheck.class);
        SeContainer container = initializer.initialize();
        SmallRyeReactiveMessagingLivenessCheck check = container.getBeanManager().createInstance()
                .select(SmallRyeReactiveMessagingLivenessCheck.class, Liveness.Literal.INSTANCE).get();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
        assertThat(check.call().getData().orElse(null)).containsExactly(entry("my-channel", "[OK]"));

        MyReporterA a = container.getBeanManager().createInstance()
                .select(MyReporterA.class, ConnectorLiteral.of("connector-a")).get();

        MyReporterB b = container.getBeanManager().createInstance()
                .select(MyReporterB.class, ConnectorLiteral.of("connector-b")).get();

        a.toggle();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.UP);

        b.toggle();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
        assertThat(check.call().getData().orElse(null)).containsExactly(entry("my-channel", "[KO]"));
    }

    @Test
    public void testWithNoConnector() {
        SeContainerInitializer initializer = SeContainerInitializer.newInstance().disableDiscovery();
        initializer.addBeanClasses(HealthCenter.class, SmallRyeReactiveMessagingLivenessCheck.class);
        SeContainer container = initializer.initialize();
        SmallRyeReactiveMessagingLivenessCheck check = container.getBeanManager().createInstance()
                .select(SmallRyeReactiveMessagingLivenessCheck.class, Liveness.Literal.INSTANCE).get();

        assertThat(check.call().getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
        assertThat(check.call().getData()).isEmpty();
    }

    @ApplicationScoped
    @Connector("connector-a")
    private static class MyReporterA implements HealthReporter {

        boolean ok = true;

        @Override
        public HealthReport getReadiness() {
            return HealthReport.builder().add("my-channel", ok).build();
        }

        public void toggle() {
            ok = !ok;
        }
    }

    @ApplicationScoped
    @Connector("connector-b")
    private static class MyReporterB implements HealthReporter {

        boolean ok = true;

        @Override
        public HealthReport getLiveness() {
            return HealthReport.builder().add("my-channel", ok).build();
        }

        public void toggle() {
            ok = !ok;
        }
    }

}
