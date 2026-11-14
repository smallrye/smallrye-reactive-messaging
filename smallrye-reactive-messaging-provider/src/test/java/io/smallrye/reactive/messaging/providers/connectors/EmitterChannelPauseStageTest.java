package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Properties;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

/**
 * An emitter channel bound to an outbound connector is decorated on the emitter side and on the connector side: it
 * must end up with a single pause stage, which still completes the emitter on drain.
 */
class EmitterChannelPauseStageTest extends WeldTestBaseWithoutTails {

    @AfterEach
    void cleanup() {
        releaseConfig();
    }

    @Test
    void emitterChannelHasOnePausableThatCompletesTheEmitter() {
        Properties props = new Properties();
        props.put("mp.messaging.outgoing.out.connector", "smallrye-dummy");
        props.put("mp.messaging.outgoing.out.graceful-shutdown", "true");
        installConfigFromProperties(props);
        addBeanClass(EmitterBean.class);
        initialize();

        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy"))
                .get();
        get(EmitterBean.class).send("hello");
        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("hello"));

        ChannelRegistry registry = get(ChannelRegistry.class);
        assertThat(registry.getPausableChannels()).containsOnlyKeys("out");
        PausableChannel pausable = registry.getPausable("out");

        pausable.pauseAndDrain().await().atMost(Duration.ofSeconds(5));

        // The drain completed the emitter, and the completion reached the connector's sink.
        await().until(connector::gotCompletion);
    }

    @ApplicationScoped
    public static class EmitterBean {

        @Inject
        @Channel("out")
        Emitter<String> emitter;

        public void send(String payload) {
            emitter.send(payload);
        }
    }
}
