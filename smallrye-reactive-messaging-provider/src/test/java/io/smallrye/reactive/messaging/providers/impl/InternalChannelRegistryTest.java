package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.providers.helpers.IgnoringSubscriber;

class InternalChannelRegistryTest {

    private InternalChannelRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new InternalChannelRegistry();
    }

    @Test
    void connectorNameStoredSeparatelyForIncomingAndOutgoing() {
        Flow.Publisher<Message<?>> publisher = Multi.createFrom().empty();
        Flow.Subscriber<Message<?>> subscriber = IgnoringSubscriber.INSTANCE;

        registry.register("my-channel", "connector-in", publisher, false);
        registry.register("my-channel", "connector-out", subscriber, false);

        assertThat(registry.getIncomingConnectorName("my-channel")).isEqualTo("connector-in");
        assertThat(registry.getOutgoingConnectorName("my-channel")).isEqualTo("connector-out");
    }

    @Test
    void connectorNamesReturnsAll() {
        Flow.Publisher<Message<?>> publisher = Multi.createFrom().empty();
        Flow.Subscriber<Message<?>> subscriber = IgnoringSubscriber.INSTANCE;

        registry.register("in-channel", "connector-a", publisher, false);
        registry.register("out-channel", "connector-b", subscriber, false);

        assertThat(registry.getConnectorNames())
                .containsEntry("in-channel", "connector-a")
                .containsEntry("out-channel", "connector-b")
                .hasSize(2);
    }

    @Test
    void nullConnectorNameIsNotStored() {
        Flow.Publisher<Message<?>> publisher = Multi.createFrom().empty();
        Flow.Subscriber<Message<?>> subscriber = IgnoringSubscriber.INSTANCE;

        registry.register("channel-a", (String) null, publisher, false);
        registry.register("channel-b", (String) null, subscriber, false);

        assertThat(registry.getIncomingConnectorName("channel-a")).isNull();
        assertThat(registry.getOutgoingConnectorName("channel-b")).isNull();
        assertThat(registry.getConnectorNames()).isEmpty();
    }

    @Test
    void registerWithoutConnectorNameDoesNotStoreConnector() {
        Flow.Publisher<Message<?>> publisher = Multi.createFrom().empty();
        Flow.Subscriber<Message<?>> subscriber = IgnoringSubscriber.INSTANCE;

        registry.register("channel-a", publisher, false);
        registry.register("channel-b", subscriber, false);

        assertThat(registry.getIncomingConnectorName("channel-a")).isNull();
        assertThat(registry.getOutgoingConnectorName("channel-b")).isNull();
    }
}
