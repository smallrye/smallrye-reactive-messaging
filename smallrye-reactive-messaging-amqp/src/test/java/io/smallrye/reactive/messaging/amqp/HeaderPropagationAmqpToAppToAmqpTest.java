package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;

public class HeaderPropagationAmqpToAppToAmqpTest extends AmqpBrokerTestBase {

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
    public void testFromAmqpToAppToAmqp() {

        String address = UUID.randomUUID().toString();
        String source = UUID.randomUUID().toString();
        List<io.vertx.mutiny.amqp.AmqpMessage> messages = new CopyOnWriteArrayList<>();

        weld.addBeanClass(MyAppProcessingData.class);

        usage.consume(address, messages::add);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.durable", true)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)

                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", source)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .write();

        container = weld.initialize();

        AtomicInteger count = new AtomicInteger();
        usage.produce(source, 10, count::getAndIncrement);

        await()
                .pollDelay(1, TimeUnit.SECONDS)
                .until(() -> {
                    // We may have missed a few messages, so retry.
                    int size = messages.size();
                    int missing = 10 - size;
                    if (missing > 0) {
                        usage.produce(source, missing, count::getAndIncrement);
                    }
                    return missing <= 0;
                });
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.subject()).isEqualTo("test");
            assertThat(entry.applicationProperties().getString("X-Header")).isEqualTo("value");
            assertThat(entry.address()).isEqualTo(address);
        });
    }

    @ApplicationScoped
    public static class MyAppProcessingData {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return input.addMetadata(
                    OutgoingAmqpMetadata.builder()
                            .withApplicationProperties(new JsonObject().put("X-Header", "value"))
                            .withSubject("test")
                            .build());
        }

        @Incoming("p1")
        @Outgoing("amqp")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

}
