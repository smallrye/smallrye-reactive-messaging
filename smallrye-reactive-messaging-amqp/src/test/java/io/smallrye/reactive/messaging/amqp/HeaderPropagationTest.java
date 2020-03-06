package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.reactivex.Flowable;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.vertx.core.json.JsonObject;

public class HeaderPropagationTest extends AmqpTestBase {

    private WeldContainer container;
    private Weld weld = new Weld();

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        System.clearProperty("mp-config");
    }

    @Test
    public void testFromAppToAmqp() {
        List<io.vertx.mutiny.amqp.AmqpMessage> messages = new CopyOnWriteArrayList<>();

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(MyAppGeneratingData.class);

        System.setProperty("mp-config", "app-generating-data");

        usage.consume("my-address", messages::add);
        container = weld.initialize();

        await().until(() -> messages.size() >= 10);
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.subject()).isEqualTo("test");
            assertThat(entry.applicationProperties().getString("X-Header")).isEqualTo("value");
            assertThat(entry.address()).isEqualTo("my-address");
        });
    }

    @Test
    public void testFromAmqpToAppToAmqp() {
        List<io.vertx.mutiny.amqp.AmqpMessage> messages = new CopyOnWriteArrayList<>();

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(MyAppProcessingData.class);

        System.setProperty("mp-config", "app-processing-data");

        usage.consume("my-address", messages::add);

        container = weld.initialize();

        AtomicInteger count = new AtomicInteger();
        usage.produce("my-source", 20, count::getAndIncrement);

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
        public Flowable<Integer> source() {
            return Flowable.range(0, 10);
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return AmqpMessage.<Integer> builder()
                    .withAddress("my-address")
                    .withIntegerAsBody(input.getPayload())
                    .withApplicationProperties(new JsonObject().put("X-Header", "value"))
                    .withSubject("test")
                    .build();
        }

        @Incoming("p1")
        @Outgoing("amqp")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingData {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return AmqpMessage.<Integer> builder()
                    .withIntegerAsBody(input.getPayload())
                    .withApplicationProperties(new JsonObject().put("X-Header", "value"))
                    .withSubject("test")
                    .build();
        }

        @Incoming("p1")
        @Outgoing("amqp")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

}
