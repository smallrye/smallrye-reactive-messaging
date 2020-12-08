package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;

public class ObjectExchangeTest extends AmqpBrokerTestBase {

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
    public void testServicesExchangingStructuresAsJson() {
        weld.addBeanClass(Consumer.class);
        weld.addBeanClass(Generator.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.to-amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.to-amqp.address", "prices")
                .put("mp.messaging.outgoing.to-amqp.durable", false)
                .put("mp.messaging.outgoing.to-amqp.host", host)
                .put("mp.messaging.outgoing.to-amqp.port", port)

                .put("mp.messaging.incoming.from-amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.from-amqp.address", "prices")
                .put("mp.messaging.incoming.from-amqp.durable", true)
                .put("mp.messaging.incoming.from-amqp.host", host)
                .put("mp.messaging.incoming.from-amqp.port", port)

                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        container = weld.initialize();

        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        Generator generator = container.getBeanManager().createInstance().select(Generator.class).get();
        Consumer consumer = container.getBeanManager().createInstance().select(Consumer.class).get();
        generator.send();
        await().until(() -> consumer.list().size() == 2);

        assertThat(consumer.list()).allSatisfy(p -> {
            assertThat(p).isNotNull();
            assertThat(p.getPrice()).isGreaterThan(0);
        });
    }

    public static class Price {

        private int price;

        public int getPrice() {
            return price;
        }

        public Price setPrice(int price) {
            this.price = price;
            return this;
        }
    }

    @ApplicationScoped
    public static class Generator {

        @Inject
        @Channel("to-amqp")
        Emitter<Price> emitter;

        public void send() {
            emitter.send(new Price().setPrice(1));
            emitter.send(new Price().setPrice(2));
        }

    }

    @ApplicationScoped
    public static class Consumer {

        List<Price> prices = new CopyOnWriteArrayList<>();

        @Incoming("from-amqp")
        public void consume(JsonObject p) {
            Price price = p.mapTo(Price.class);
            prices.add(price);
        }

        public List<Price> list() {
            return prices;
        }
    }

}
