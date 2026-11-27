package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Queue;

import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.support.JmsTestBase;

/**
 * Closing a channel created at runtime through the {@link ChannelFactory} stops its consumer at the connector level, so
 * the channel's name can be used again without two consumers sharing the queue.
 */
class JmsRuntimeChannelCloseTest extends JmsTestBase {

    private static final String QUEUE = "runtime-close-queue";

    private JmsConnectorIncomingConfiguration incoming(String channel) {
        return JmsConnectorIncomingConfiguration.builder(channel)
                .destination(QUEUE)
                .tracingEnabled(false)
                .build();
    }

    @Test
    void closingARuntimeChannelStopsItsConsumerAndFreesItsName() {
        addConfig(null);
        WeldContainer container = deploy();
        ChannelFactory factory = container.select(ChannelFactory.class).get();
        ChannelLifecycleManager lifecycle = container.select(ChannelLifecycleManager.class).get();
        ConnectionFactory cf = container.select(ConnectionFactory.class).get();

        List<Integer> first = new CopyOnWriteArrayList<>();
        factory.incoming(incoming("runtime-in"), Integer.class).subscribe(payload -> {
            first.add(payload);
        });
        send(cf, 1, 2, 3);
        await().untilAsserted(() -> assertThat(first).containsExactly(1, 2, 3));

        lifecycle.closeIncoming("runtime-in").await().atMost(Duration.ofSeconds(10));

        // The closed channel's consumer is gone: these stay on the queue.
        send(cf, 4, 5, 6);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(first).containsExactly(1, 2, 3));

        // The name is free again, and the new channel is the only consumer of the queue.
        List<Integer> second = new CopyOnWriteArrayList<>();
        factory.incoming(incoming("runtime-in"), Integer.class).subscribe(payload -> {
            second.add(payload);
        });
        await().untilAsserted(() -> assertThat(second).containsExactly(4, 5, 6));
        assertThat(first).containsExactly(1, 2, 3);
    }

    private static void send(ConnectionFactory cf, int... payloads) {
        try (JMSContext context = cf.createContext()) {
            JMSProducer producer = context.createProducer();
            Queue queue = context.createQueue(QUEUE);
            for (int payload : payloads) {
                producer.send(queue, context.createObjectMessage(payload));
            }
        }
    }
}
