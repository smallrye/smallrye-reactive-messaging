package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.rabbitmq.converter.JsonValueMessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.converter.StringMessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.converter.TypeMessageConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;

@SuppressWarnings("ConstantConditions")
class MessageConvertersTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            get(container, RabbitMQConnector.class,
                    ConnectorLiteral.of(RabbitMQConnector.CONNECTOR_NAME))
                    .terminate(null);
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testJsonObjectConverter() {
        init(weld, getConfig(HttpHeaderValues.APPLICATION_JSON.toString()));
        weld.addBeanClass(JsonObjectConsumer.class);

        container = weld.initialize();

        await().until(() -> isRabbitMQConnectorAvailable(container));
        JsonObjectConsumer bean = get(container, JsonObjectConsumer.class);

        await().until(() -> isRabbitMQConnectorAvailable(container));

        List<JsonObject> list = bean.counts();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produce(exchangeName, queueName, queueName, 10,
                () -> JsonObject.of("count", counter.getAndIncrement()).toString(), "application/json")).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.counts().size() >= 10);
        assertThat(bean.counts())
                .extracting(j -> j.getInteger("count"))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testStringConverter() {
        init(weld, getConfig(HttpHeaderValues.TEXT_PLAIN.toString()));
        weld.addBeanClass(StringConsumer.class);

        container = weld.initialize();

        await().until(() -> isRabbitMQConnectorAvailable(container));
        StringConsumer bean = get(container, StringConsumer.class);

        await().until(() -> isRabbitMQConnectorAvailable(container));

        assertThat(bean.counts()).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produce(exchangeName, queueName, queueName, 10,
                () -> String.valueOf(counter.getAndIncrement()), HttpHeaderValues.TEXT_PLAIN.toString())).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.counts().size() >= 10);
        assertThat(bean.counts())
                .extracting(s -> Integer.valueOf(s))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testTypeConverter() {
        init(weld, getConfig(HttpHeaderValues.APPLICATION_JSON.toString()));
        weld.addBeanClass(TypeConsumer.class);
        weld.addBeanClass(JsonbMapping.class);

        container = weld.initialize();

        await().until(() -> isRabbitMQConnectorAvailable(container));
        TypeConsumer bean = get(container, TypeConsumer.class);

        await().until(() -> isRabbitMQConnectorAvailable(container));

        assertThat(bean.counts()).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produce(exchangeName, queueName, queueName, 10,
                () -> JsonObject.of("count", counter.getAndIncrement()).toString(),
                HttpHeaderValues.APPLICATION_JSON.toString())).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.counts().size() >= 10);
        assertThat(bean.counts())
                .extracting(j -> j.getCount())
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private static void init(Weld weld, MapBasedConfig config) {
        config.write();
        weld.addBeanClass(TypeMessageConverter.class);
        weld.addBeanClass(JsonValueMessageConverter.class);
        weld.addBeanClass(StringMessageConverter.class);
    }

    private MapBasedConfig getConfig(String contentTypeOverride) {
        String count = "mp.messaging.incoming.count.";
        Map<String, Object> config = new HashMap<>();
        config.put(count + "exchange.name", exchangeName);
        config.put(count + "queue.name", queueName);
        config.put(count + "connector", RabbitMQConnector.CONNECTOR_NAME);
        config.put(count + "host", host);
        config.put(count + "port", port);
        config.put(count + "tracing-enabled", false);
        if (contentTypeOverride != null) {
            config.put(count + "content-type-override", contentTypeOverride);
        }
        return new MapBasedConfig(config);
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

    @ApplicationScoped
    public static class TypeConsumer {

        List<Count> counts = new CopyOnWriteArrayList<>();

        @Incoming("count")
        public void processCount(Count count) {
            counts.add(count);
        }

        public List<Count> counts() {
            return counts;
        }
    }

    public static class Count {

        private Integer count;

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }
    }

    @ApplicationScoped
    public static class JsonbMapping implements JsonMapping {

        private Jsonb jsonb = JsonbBuilder.create();

        @Override
        public String toJson(Object object) {
            return jsonb.toJson(object);
        }

        @Override
        public <T> T fromJson(String str, Class<T> type) {
            return jsonb.fromJson(str, type);
        }

        @Override
        public <T> T fromJson(String str, Type type) {
            return jsonb.fromJson(str, type);
        }
    }

}
