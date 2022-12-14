package io.smallrye.reactive.messaging.providers.extension;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;

public class HealthCenterTest {

    @Test
    public void testWithTwoConnectors() {
        SeContainerInitializer initializer = SeContainerInitializer.newInstance().disableDiscovery();
        initializer.addBeanClasses(HealthCenter.class, MyReporterA.class, MyReporterB.class);
        SeContainer container = initializer.initialize();
        HealthCenter center = container.getBeanManager().createInstance().select(HealthCenter.class).get();

        assertThat(center.getLiveness().isOk()).isTrue();
        assertThat(center.getLiveness().getChannels()).hasSize(1);
        assertThat(center.getReadiness().isOk()).isTrue();
        assertThat(center.getReadiness().getChannels()).hasSize(1);

        MyReporterA a = container.getBeanManager().createInstance()
                .select(MyReporterA.class, ConnectorLiteral.of("connector-a")).get();

        MyReporterB b = container.getBeanManager().createInstance()
                .select(MyReporterB.class, ConnectorLiteral.of("connector-b")).get();

        a.toggle();

        assertThat(center.getReadiness().isOk()).isFalse();
        assertThat(center.getLiveness().isOk()).isTrue();

        b.toggle();

        assertThat(center.getLiveness().isOk()).isFalse();
        assertThat(center.getReadiness().isOk()).isFalse();
    }

    @Test
    public void testWithNoConnector() {
        SeContainerInitializer initializer = SeContainerInitializer.newInstance().disableDiscovery();
        initializer.addBeanClasses(HealthCenter.class);
        SeContainer container = initializer.initialize();
        HealthCenter center = container.getBeanManager().createInstance().select(HealthCenter.class).get();

        assertThat(center.getLiveness().isOk()).isTrue();
        assertThat(center.getLiveness().getChannels()).isEmpty();
        assertThat(center.getReadiness().isOk()).isTrue();
        assertThat(center.getReadiness().getChannels()).isEmpty();
    }

    @Test
    public void testWithFailureReporting() {
        SeContainerInitializer initializer = SeContainerInitializer.newInstance().disableDiscovery();
        initializer.addBeanClasses(HealthCenter.class);
        SeContainer container = initializer.initialize();
        HealthCenter center = container.getBeanManager().createInstance().select(HealthCenter.class).get();

        assertThat(center.getLiveness().isOk()).isTrue();
        assertThat(center.getLiveness().getChannels()).isEmpty();
        assertThat(center.getReadiness().isOk()).isTrue();
        assertThat(center.getReadiness().getChannels()).isEmpty();

        center.report("my-channel", new IOException("boom"));

        assertThat(center.getLiveness().isOk()).isFalse();
        assertThat(center.getLiveness().getChannels()).hasSize(1);
        assertThat(center.getReadiness().isOk()).isFalse();
        assertThat(center.getReadiness().getChannels()).hasSize(1);
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
