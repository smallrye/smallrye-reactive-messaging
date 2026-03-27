package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;

@SuppressWarnings("ConstantConditions")
class MessageConvertersTest extends WeldTestBase {

    @Test
    public void testJsonObjectConverter() {
        weld.addBeanClass(JsonObjectConsumer.class);

        MapBasedConfig config = getConfig(HttpHeaderValues.APPLICATION_JSON.toString());
        JsonObjectConsumer bean = runApplication(config, JsonObjectConsumer.class);

        List<JsonObject> list = bean.counts();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produce(exchangeName, queueName, queueName, 10,
                () -> JsonObject.of("count", counter.getAndIncrement()).toString(), "application/json");

        await().until(() -> bean.counts().size() >= 10);
        assertThat(bean.counts())
                .extracting(j -> j.getInteger("count"))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testStringConverter() {
        weld.addBeanClass(StringConsumer.class);

        MapBasedConfig config = getConfig(HttpHeaderValues.TEXT_PLAIN.toString());
        StringConsumer bean = runApplication(config, StringConsumer.class);

        assertThat(bean.counts()).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produce(exchangeName, queueName, queueName, 10,
                () -> String.valueOf(counter.getAndIncrement()), HttpHeaderValues.TEXT_PLAIN.toString());

        await().until(() -> bean.counts().size() >= 10);
        assertThat(bean.counts())
                .extracting(Integer::valueOf)
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private MapBasedConfig getConfig(String contentTypeOverride) {
        MapBasedConfig config = commonConfig()
                .with("mp.messaging.incoming.count.exchange.name", exchangeName)
                .with("mp.messaging.incoming.count.queue.name", queueName)
                .with("mp.messaging.incoming.count.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.count.host", host)
                .with("mp.messaging.incoming.count.port", port)
                .with("mp.messaging.incoming.count.tracing-enabled", false);
        if (contentTypeOverride != null) {
            config.with("mp.messaging.incoming.count.content-type-override", contentTypeOverride);
        }
        return config;
    }

    @ApplicationScoped
    public static class JsonObjectConsumer {

        List<JsonObject> counts = new CopyOnWriteArrayList<>();

        @Incoming("count")
        public void processCount(JsonObject count) {
            counts.add(count);
        }

        public List<JsonObject> counts() {
            return counts;
        }
    }

    @ApplicationScoped
    public static class StringConsumer {

        List<String> counts = new CopyOnWriteArrayList<>();

        @Incoming("count")
        public void processCount(String count) {
            counts.add(count);
        }

        public List<String> counts() {
            return counts;
        }
    }

}
