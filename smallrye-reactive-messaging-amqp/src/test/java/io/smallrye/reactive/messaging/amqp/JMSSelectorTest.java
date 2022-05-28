package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessageBuilder;

public class JMSSelectorTest extends AmqpBrokerTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());

        System.clearProperty("mp-config");
        System.clearProperty("client-options-name");
        System.clearProperty("amqp-client-options-name");
    }

    @Test
    public void test() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", "data")
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.selector", "x='bar'")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .write();

        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));
        ConsumptionBean bean = container.getBeanManager().createInstance().select(ConsumptionBean.class).get();

        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produce("data", 10, () -> {
            int v = counter.getAndIncrement();
            String x = "bar";
            if (v % 2 == 1) {
                x = "baz";
            }
            AmqpMessageBuilder builder = io.vertx.mutiny.amqp.AmqpMessage.create()
                    .durable(false)
                    .ttl(10000)
                    .address("data")
                    .applicationProperties(new JsonObject().put("x", x))
                    .withIntegerAsBody(v);
            return builder.build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 5);
        assertThat(list).containsExactly(1, 3, 5, 7, 9); // Remember, val + 1 in the consumer
    }

}
