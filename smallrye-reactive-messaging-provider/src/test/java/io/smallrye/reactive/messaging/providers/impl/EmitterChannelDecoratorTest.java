package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.IncomingInterceptor;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;
import io.smallrye.reactive.messaging.providers.connectors.MyDummyConnector;

/**
 * Emitter channels are not connector channels, even when bound to an outbound connector: publisher decorators get a
 * {@code null} configuration for them, as in 5.0.0. Decorators branching on {@code isConnector}, such as the incoming
 * interceptor decorator, and decorators overriding {@code decorate(Multi, List, Config)} and checking the configuration
 * against {@code null}, must not treat them as connector channels.
 */
class EmitterChannelDecoratorTest extends WeldTestBaseWithoutTails {

    @AfterEach
    void cleanup() {
        releaseConfig();
    }

    @Test
    void emitterChannelInWiringIsNotIntercepted() {
        Properties props = new Properties();
        props.put("mp.messaging.outgoing.wired-out.connector", "smallrye-dummy");
        installConfigFromProperties(props);
        addBeanClass(RecordingInterceptor.class, RecordingConfigDecorator.class, EmitterBean.class);
        initialize();

        get(EmitterBean.class).send("hello");

        await().untilAsserted(() -> assertThat(connector().list()).contains("hello"));
        assertThat(get(RecordingInterceptor.class).intercepted()).isEmpty();
        assertThat(get(RecordingConfigDecorator.class).configPresent()).containsEntry("wired-out", false);
    }

    @Test
    @SuppressWarnings("unchecked")
    void emitterChannelFromChannelFactoryIsNotIntercepted() {
        addBeanClass(RecordingInterceptor.class, RecordingConfigDecorator.class);
        initialize();

        Emitter<String> emitter = get(ChannelFactory.class).outgoing(outgoingConfig("factory-out"), Emitter.class);
        emitter.send("hi");

        await().untilAsserted(() -> assertThat(connector().list()).contains("hi"));
        assertThat(get(RecordingInterceptor.class).intercepted()).isEmpty();
        assertThat(get(RecordingConfigDecorator.class).configPresent()).containsEntry("factory-out", false);
    }

    @Test
    void incomingConnectorChannelIsStillIntercepted() {
        addBeanClass(RecordingInterceptor.class, RecordingConfigDecorator.class);
        initialize();

        List<Integer> received = new CopyOnWriteArrayList<>();
        get(ChannelFactory.class).incoming(incomingConfig("factory-in"), Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).hasSize(3));
        assertThat(get(RecordingInterceptor.class).intercepted()).hasSize(3);
        assertThat(get(RecordingConfigDecorator.class).configPresent()).containsEntry("factory-in", true);
    }

    private MyDummyConnector connector() {
        return container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();
    }

    private static IncomingConnectorConfiguration incomingConfig(String channel) {
        var config = new BuilderConfig(Map.of(
                "connector", "dummy",
                "channel-name", channel));
        return new IncomingConnectorConfiguration() {
            @Override
            public Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }

    private static OutgoingConnectorConfiguration outgoingConfig(String channel) {
        var config = new BuilderConfig(Map.of(
                "connector", "dummy",
                "channel-name", channel));
        return new OutgoingConnectorConfiguration() {
            @Override
            public Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }

    @ApplicationScoped
    public static class EmitterBean {

        @Inject
        @Channel("wired-out")
        Emitter<String> emitter;

        public void send(String payload) {
            emitter.send(payload);
        }
    }

    /**
     * Records, for each decorated publisher, whether it got a configuration, like decorators that treat a non-null
     * configuration as a connector channel.
     */
    @ApplicationScoped
    public static class RecordingConfigDecorator implements PublisherDecorator {

        private final Map<String, Boolean> configPresent = new ConcurrentHashMap<>();

        @Override
        public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
                Config channelConfig) {
            channelName.forEach(name -> configPresent.put(name, channelConfig != null));
            return publisher;
        }

        // A method, not a field: the test reads it through the client proxy.
        public Map<String, Boolean> configPresent() {
            return configPresent;
        }
    }

    /**
     * A {@code @Default} incoming interceptor: applied to every connector incoming channel without an identified one.
     */
    @ApplicationScoped
    public static class RecordingInterceptor implements IncomingInterceptor {

        private final List<Object> intercepted = new CopyOnWriteArrayList<>();

        @Override
        public Message<?> afterMessageReceive(Message<?> message) {
            intercepted.add(message.getPayload());
            return message;
        }

        @Override
        public void onMessageAck(Message<?> message) {
        }

        @Override
        public void onMessageNack(Message<?> message, Throwable failure) {
        }

        public List<Object> intercepted() {
            return intercepted;
        }
    }
}
