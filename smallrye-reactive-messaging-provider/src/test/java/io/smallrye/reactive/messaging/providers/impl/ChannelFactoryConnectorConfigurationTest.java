package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.MapBasedConfig;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;
import io.smallrye.reactive.messaging.providers.connectors.MyDummyConnector;

class ChannelFactoryConnectorConfigurationTest extends WeldTestBaseWithoutTails {

    private IncomingConnectorConfiguration incomingConfig(String channel) {
        return incomingConfig(channel, Map.of());
    }

    private IncomingConnectorConfiguration incomingConfig(String channel, Map<String, String> extra) {
        var values = new java.util.HashMap<>(Map.of(
                "connector", "dummy",
                "channel-name", channel));
        values.putAll(extra);
        var config = new MapBasedConfig(values);
        return new IncomingConnectorConfiguration() {
            @Override
            public org.eclipse.microprofile.config.Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }

    private OutgoingConnectorConfiguration outgoingConfig(String channel) {
        var config = new MapBasedConfig(Map.of(
                "connector", "dummy",
                "channel-name", channel));
        return new OutgoingConnectorConfiguration() {
            @Override
            public org.eclipse.microprofile.config.Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }

    @Test
    void incomingWithConnectorConfiguration() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        factory.incoming(incomingConfig("test-in"), Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        assertThat(registry.getIncomingConnectorName("test-in")).isEqualTo("smallrye-dummy");
    }

    @Test
    void incomingWithConnectorConfigurationAndCustomAttribute() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        factory.incoming(incomingConfig("test-in", Map.of("increment", "10")), Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(11, 12, 13));
    }

    @Test
    void outgoingWithEmitterConnectorConfiguration() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        MutinyEmitter<String> emitter = factory.outgoing(outgoingConfig("test-out"), MutinyEmitter.class);

        assertThat(emitter).isNotNull();
        assertThat(registry.getSubscribers("test-out")).isNotEmpty();
        assertThat(registry.getOutgoingConnectorName("test-out")).isEqualTo("smallrye-dummy");
    }

    @Test
    void outgoingWithEmitterSendsMessagesConnectorConfiguration() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Emitter<String> emitter = factory.outgoing(outgoingConfig("test-out"), Emitter.class);

        emitter.send("hello");
        emitter.send("world");

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("hello", "world"));
    }

    @Test
    void outgoingWithSourcePublisherConnectorConfiguration() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Flow.Publisher<Message<?>> source = Multi.createFrom().items("a", "b", "c")
                .map(Message::of);

        factory.outgoing(outgoingConfig("test-out-src"), source);

        assertThat(registry.getOutgoingConnectorName("test-out-src")).isEqualTo("smallrye-dummy");
        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("a", "b", "c"));
    }

    @Test
    void outgoingSubscriberConnectorConfiguration() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing(outgoingConfig("test-out"));

        assertThat(subscriber).isNotNull();
        assertThat(registry.getSubscribers("test-out")).isNotEmpty();
        assertThat(registry.getOutgoingConnectorName("test-out")).isEqualTo("smallrye-dummy");
    }
}
