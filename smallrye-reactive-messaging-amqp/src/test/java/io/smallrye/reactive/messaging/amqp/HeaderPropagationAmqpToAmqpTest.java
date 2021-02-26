package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;

public class HeaderPropagationAmqpToAmqpTest extends AmqpBrokerTestBase {

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
    public void testFromAppToAmqp() {
        List<io.vertx.mutiny.amqp.AmqpMessage> messages = new CopyOnWriteArrayList<>();

        weld.addBeanClass(MyAppGeneratingData.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.amqp.address", "should-not-be-used")
                .put("mp.messaging.outgoing.amqp.durable", false)
                .put("mp.messaging.outgoing.amqp.host", host)
                .put("mp.messaging.outgoing.amqp.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        usage.consume("my-address", messages::add);
        container = weld.initialize();

        await().until(() -> messages.size() >= 10);
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.subject()).isEqualTo("test");
            assertThat(entry.applicationProperties().getString("X-Header")).isEqualTo("value");
            assertThat(entry.address()).isEqualTo("my-address");
        });
    }

    @ApplicationScoped
    public static class MyAppGeneratingData {

        @Outgoing("source")
        public Multi<Integer> source() {
            return Multi.createFrom().range(0, 11);
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            OutgoingAmqpMetadata metadata = OutgoingAmqpMetadata.builder()
                    .withAddress("my-address")
                    .withApplicationProperties(new JsonObject().put("X-Header", "value"))
                    .withSubject("test")
                    .build();
            return input.addMetadata(metadata);
        }

        @Incoming("p1")
        @Outgoing("amqp")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

}
