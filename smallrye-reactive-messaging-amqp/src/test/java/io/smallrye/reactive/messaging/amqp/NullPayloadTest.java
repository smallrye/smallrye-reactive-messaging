package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class NullPayloadTest extends AmqpBrokerTestBase {

    private WeldContainer container;
    private final Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testNullPayload() {
        weld.addBeanClass(NullProducingBean.class);

        List<String> bodies = new CopyOnWriteArrayList<>();
        usage.consume("sink", v -> bodies.add(v.bodyAsString()));

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", true)
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        container = weld.initialize();

        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        await().untilAsserted(() -> assertThat(bodies).hasSize(10));
        assertThat(bodies).containsOnlyNulls();

    }
}
