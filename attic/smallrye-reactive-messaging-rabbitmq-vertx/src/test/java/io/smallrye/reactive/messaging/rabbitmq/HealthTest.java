package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class HealthTest extends WeldTestBase {

    private MapBasedConfig getBaseConfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.in.queue.name", "in")
                .with("mp.messaging.incoming.in.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue.name", "out")
                .with("mp.messaging.outgoing.out.connector", RabbitMQConnector.CONNECTOR_NAME);
    }

    @Test
    void testReadinessAndLivenessEnabled() {
        MapBasedConfig config = getBaseConfig();

        addBeans(MyApp.class);
        runApplication(config);
        HealthCenter health = get(container, HealthCenter.class);
        assertThat(health.getLiveness().isOk()).isTrue();
        assertThat(health.getLiveness().getChannels()).anySatisfy(ci -> {
            assertThat(ci.getChannel()).isEqualTo("in");
            assertThat(ci.isOk()).isTrue();
        })
                .anySatisfy(ci -> {
                    assertThat(ci.getChannel()).isEqualTo("out");
                    assertThat(ci.isOk()).isTrue();
                });

        assertThat(health.getReadiness().isOk()).isTrue();
        assertThat(health.getReadiness().getChannels()).anySatisfy(ci -> {
            assertThat(ci.getChannel()).isEqualTo("in");
            assertThat(ci.isOk()).isTrue();
        })
                .anySatisfy(ci -> {
                    assertThat(ci.getChannel()).isEqualTo("out");
                    assertThat(ci.isOk()).isTrue();
                });
    }

    @Test
    void testHealthDisabled() {
        MapBasedConfig config = getBaseConfig()
                .with("mp.messaging.incoming.in.health-enabled", false)
                .with("mp.messaging.outgoing.out.health-enabled", false);

        addBeans(MyApp.class);
        runApplication(config);
        HealthCenter health = get(container, HealthCenter.class);
        assertThat(health.getLiveness().isOk()).isTrue();
        assertThat(health.getLiveness().getChannels()).isEmpty();

        assertThat(health.getReadiness().isOk()).isTrue();
        assertThat(health.getReadiness().getChannels()).isEmpty();
    }

    @Test
    void testReadinessDisabled() {
        MapBasedConfig config = getBaseConfig()
                .with("mp.messaging.incoming.in.health-readiness-enabled", false)
                .with("mp.messaging.outgoing.out.health-readiness-enabled", false);

        addBeans(MyApp.class);
        runApplication(config);
        HealthCenter health = get(container, HealthCenter.class);
        assertThat(health.getLiveness().isOk()).isTrue();
        assertThat(health.getLiveness().getChannels()).hasSize(2);

        assertThat(health.getReadiness().isOk()).isTrue();
        assertThat(health.getReadiness().getChannels()).isEmpty();
    }

    @Test
    void testWithAppUsingChannels() {
        MapBasedConfig config = getBaseConfig()
                .with("mp.messaging.incoming.in.health-lazy-subscription", true);

        addBeans(MyAppUsingChannels.class);
        runApplication(config);
        HealthCenter health = get(container, HealthCenter.class);
        assertThat(health.getLiveness().isOk()).isTrue();
        assertThat(health.getLiveness().getChannels()).anySatisfy(ci -> {
            assertThat(ci.getChannel()).isEqualTo("in");
            assertThat(ci.isOk()).isTrue();
        })
                .anySatisfy(ci -> {
                    assertThat(ci.getChannel()).isEqualTo("out");
                    assertThat(ci.isOk()).isTrue();
                });

        assertThat(health.getReadiness().isOk()).isTrue();
        assertThat(health.getReadiness().getChannels()).anySatisfy(ci -> {
            assertThat(ci.getChannel()).isEqualTo("in");
            assertThat(ci.isOk()).isTrue();
        })
                .anySatisfy(ci -> {
                    assertThat(ci.getChannel()).isEqualTo("out");
                    assertThat(ci.isOk()).isTrue();
                });
    }

    public static class MyApp {
        @Incoming("in")
        @Outgoing("out")
        public String process(String in) {
            return in;
        }
    }

    public static class MyAppUsingChannels {

        @Inject
        @Channel("in")
        Multi<String> in;

        @Inject
        @Channel("out")
        Emitter<String> out;

    }
}
