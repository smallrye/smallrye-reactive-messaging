package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.vertx.core.json.JsonObject;

public class ObjectExchangeTest extends AmqpTestBase {

    private WeldContainer container;
    private final Weld weld = new Weld();

    @After
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
                .put("mp.messaging.outgoing.to-amqp.durable", true)
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

        Consumer consumer = container.getBeanManager().createInstance().select(Consumer.class).get();
        await().until(() -> consumer.list().size() >= 2);

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

        @Outgoing("to-amqp")
        public Multi<Price> prices() {
            AtomicInteger count = new AtomicInteger();
            return Multi.createFrom().ticks().every(Duration.ofMillis(1000))
                    .map(l -> new Price().setPrice(count.incrementAndGet()))
                    .transform().byTakingFirstItems(10)
                    .on().overflow().drop();
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
