package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;

public class RabbitMQArgumentsCDIConfigTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    String queueName;

    @BeforeEach
    public void initQueue(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        queueName = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @Test
    public void testConfigByCDIQueueArguments() throws IOException {
        Weld weld = new Weld();

        weld.addBeanClass(ArgumentsConfigBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", queueName)
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.queue.arguments", "my-args")
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAlive(container));
        await().until(() -> isRabbitMQConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers("data", queueName, "", counter::getAndIncrement);

        JsonObject queue = usage.getQueue(queueName);
        assertThat(queue.getJsonObject("arguments").getMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("my-str-arg", "str-value", "my-int-arg", 4));

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConfigByCDIQueueDefaultArguments() throws IOException {
        Weld weld = new Weld();

        weld.addBeanClass(ArgumentsConfigBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", queueName)
                .with("mp.messaging.incoming.data.exchange.name", queueName)
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAlive(container));
        await().until(() -> isRabbitMQConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(queueName, queueName, "", counter::getAndIncrement);

        JsonObject queue = usage.getQueue(queueName);
        assertThat(queue.getJsonObject("arguments").getMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("default-queue-arg", "default-value"));

        JsonObject exchange = usage.getExchange(queueName);
        assertThat(exchange.getJsonObject("arguments").getMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("default-exchange-arg", "default-value"));

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConfigByCDIExchangeArguments() throws IOException {
        Weld weld = new Weld();

        weld.addBeanClass(ArgumentsConfigBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", queueName)
                .with("mp.messaging.incoming.data.exchange.name", queueName)
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.exchange.arguments", "my-args")
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAlive(container));
        await().until(() -> isRabbitMQConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(queueName, queueName, "", counter::getAndIncrement);

        JsonObject exchange = usage.getExchange(queueName);
        assertThat(exchange.getJsonObject("arguments").getMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("my-str-arg", "str-value", "my-int-arg", 4));

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConfigByCDIDLQArguments() throws IOException {
        Weld weld = new Weld();

        weld.addBeanClass(ArgumentsConfigBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        String dlqName = queueName + ".dlq";

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", queueName)
                .with("mp.messaging.incoming.data.exchange.name", queueName)
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.auto-bind-dlq", true)
                .with("mp.messaging.incoming.data.dlx.declare", true)
                .with("mp.messaging.incoming.data.dead-letter-queue.arguments", "my-args")
                .with("mp.messaging.incoming.data.dead-letter-exchange.arguments", "my-args")
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAlive(container));
        await().until(() -> isRabbitMQConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(queueName, queueName, "", counter::getAndIncrement);

        JsonObject queue = usage.getQueue(dlqName);
        assertThat(queue.getJsonObject("arguments").getMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("my-str-arg", "str-value", "my-int-arg", 4));

        JsonObject exchange = usage.getExchange("DLX");
        assertThat(exchange.getJsonObject("arguments").getMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("my-str-arg", "str-value", "my-int-arg", 4));

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

}
