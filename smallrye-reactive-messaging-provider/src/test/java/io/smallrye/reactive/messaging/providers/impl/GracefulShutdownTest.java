package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.providers.connectors.MyDummyConnector;
import io.smallrye.reactive.messaging.providers.connectors.SlowOutboundConnector;

class GracefulShutdownTest extends WeldTestBaseWithoutTails {

    @Test
    void gracefulShutdownDrainsEmitterBufferedMessages() {
        addBeanClass(EmitterBean.class);

        Properties config = new Properties();
        config.put("mp.messaging.outgoing.out.connector", "dummy");
        config.put("mp.messaging.outgoing.out.graceful-shutdown", "true");
        installConfigFromProperties(config);
        initialize();

        EmitterBean bean = get(EmitterBean.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();
        List<String> received = connector.list();

        bean.send("a");
        bean.send("b");
        bean.send("c");
        bean.send("d");
        bean.send("e");

        await().untilAsserted(() -> assertThat(received).hasSize(5));

        container.close();
        container = null;

        assertThat(received).containsExactly("a", "b", "c", "d", "e");
    }

    @Test
    void emitterRejectsNewMessagesAfterGracefulShutdown() {
        addBeanClass(EmitterBean.class);

        Properties config = new Properties();
        config.put("mp.messaging.outgoing.out.connector", "dummy");
        config.put("mp.messaging.outgoing.out.graceful-shutdown", "true");
        installConfigFromProperties(config);
        initialize();

        EmitterBean bean = get(EmitterBean.class);
        Emitter<String> emitter = bean.emitter();

        bean.send("a");
        assertThat(emitter.hasRequests()).isTrue();
        assertThat(emitter.isCancelled()).isFalse();

        container.close();
        container = null;

        assertThat(emitter.isCancelled()).isTrue();
        assertThat(emitter.hasRequests()).isFalse();
    }

    @Test
    void gracefulShutdownDrainsBufferedMessagesUnderBackpressure() {
        addBeanClass(SlowEmitterBean.class);
        addBeanClass(SlowOutboundConnector.class);

        Properties config = new Properties();
        config.put("mp.messaging.outgoing.slow-out.connector", "slow");
        config.put("mp.messaging.outgoing.slow-out.delay-ms", "300");
        config.put("mp.messaging.outgoing.slow-out.graceful-shutdown", "true");
        installConfigFromProperties(config);
        initialize();

        SlowEmitterBean bean = get(SlowEmitterBean.class);
        SlowOutboundConnector connector = container.select(SlowOutboundConnector.class,
                ConnectorLiteral.of("smallrye-slow")).get();
        List<String> received = connector.list();

        for (int i = 0; i < 10; i++) {
            bean.send("msg-" + i);
        }

        // Wait for the first message to be processed, proving the pipeline is active
        await().atMost(5, TimeUnit.SECONDS).until(() -> !received.isEmpty());

        // Verify not all messages have been processed yet — some are still in the emitter buffer
        assertThat(received.size()).as("messages should still be buffered in the emitter").isLessThan(10);
        int countBeforeShutdown = received.size();

        container.close();
        container = null;

        // All 10 messages should have been drained during shutdown
        assertThat(received).containsExactly(
                "msg-0", "msg-1", "msg-2", "msg-3", "msg-4",
                "msg-5", "msg-6", "msg-7", "msg-8", "msg-9");
    }

    @ApplicationScoped
    public static class SlowEmitterBean {

        @Inject
        @Channel("slow-out")
        Emitter<String> emitter;

        public void send(String value) {
            emitter.send(value);
        }
    }

    @ApplicationScoped
    public static class EmitterBean {

        @Inject
        @Channel("out")
        Emitter<String> emitter;

        public void send(String value) {
            emitter.send(value);
        }

        public Emitter<String> emitter() {
            return emitter;
        }
    }
}
