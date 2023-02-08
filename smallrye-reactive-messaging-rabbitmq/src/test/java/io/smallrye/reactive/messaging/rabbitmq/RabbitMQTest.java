package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@SuppressWarnings("ConstantConditions")
class RabbitMQTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.select(RabbitMQConnector.class, ConnectorLiteral.of(RabbitMQConnector.CONNECTOR_NAME)).get()
                    .terminate(null);
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    /**
     * Verifies that Exchanges are correctly declared as a result of outgoing connector
     * configuration.
     */
    @Test
    void testOutgoingDeclarations() throws Exception {

        final String exchangeName = "exchgOutgoingDeclareTest";
        final boolean exchangeDurable = false;
        final boolean exchangeAutoDelete = true;
        final String exchangeType = "fanout";

        weld.addBeanClass(OutgoingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                .put("mp.messaging.outgoing.sink.exchange.durable", exchangeDurable)
                .put("mp.messaging.outgoing.sink.exchange.auto-delete", exchangeAutoDelete)
                .put("mp.messaging.outgoing.sink.exchange.type", exchangeType)
                .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                .put("mp.messaging.outgoing.sink.exchange.declare", true)
                .put("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.tracing.enabled", false)
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        final JsonObject exchange = usage.getExchange(exchangeName);
        assertThat(exchange).isNotNull();
        assertThat(exchange.getString("name")).isEqualTo(exchangeName);
        assertThat(exchange.getString("type")).isEqualTo(exchangeType);
        assertThat(exchange.getBoolean("auto_delete")).isEqualTo(exchangeAutoDelete);
        assertThat(exchange.getBoolean("durable")).isEqualTo(exchangeDurable);
        assertThat(exchange.getBoolean("internal")).isFalse();
    }

    /**
     * Verifies that Exchanges, Queues and Bindings are correctly declared as a result of
     * incoming connector configuration.
     */
    @Test
    void testIncomingDeclarations() throws Exception {
        final String exchangeName = "exchgIncomingDeclareTest";
        final boolean exchangeDurable = false;
        final boolean exchangeAutoDelete = true;
        final String exchangeType = "fanout";

        final String queueName = "qIncomingDeclareTest";
        final boolean queueDurable = false;
        final boolean queueExclusive = true;
        final boolean queueAutoDelete = true;
        final long queueTtl = 10000L;
        final String queueType = "classic";
        final String queueMode = "default";

        final String routingKeys = "urgent, normal";

        weld.addBeanClass(IncomingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.exchange.durable", exchangeDurable)
                .put("mp.messaging.incoming.data.exchange.auto-delete", exchangeAutoDelete)
                .put("mp.messaging.incoming.data.exchange.type", exchangeType)
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.exchange.declare", true)
                .put("mp.messaging.incoming.data.queue.name", queueName)
                .put("mp.messaging.incoming.data.queue.durable", queueDurable)
                .put("mp.messaging.incoming.data.queue.exclusive", queueExclusive)
                .put("mp.messaging.incoming.data.queue.auto-delete", queueAutoDelete)
                .put("mp.messaging.incoming.data.queue.declare", true)
                .put("mp.messaging.incoming.data.queue.ttl", queueTtl)
                .put("mp.messaging.incoming.data.queue.x-queue-type", queueType)
                .put("mp.messaging.incoming.data.queue.x-queue-mode", queueMode)
                .put("mp.messaging.incoming.data.queue.single-active-consumer", true)
                .put("mp.messaging.incoming.data.routing-keys", routingKeys)
                .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("mp.messaging.incoming.data.tracing.enabled", false)
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        // verify exchange
        final JsonObject exchange = usage.getExchange(exchangeName);
        assertThat(exchange).isNotNull();
        assertThat(exchange.getString("name")).isEqualTo(exchangeName);
        assertThat(exchange.getString("type")).isEqualTo(exchangeType);
        assertThat(exchange.getBoolean("auto_delete")).isEqualTo(exchangeAutoDelete);
        assertThat(exchange.getBoolean("durable")).isEqualTo(exchangeDurable);
        assertThat(exchange.getBoolean("internal")).isFalse();

        // verify queue
        final JsonObject queue = usage.getQueue(queueName);
        assertThat(queue).isNotNull();
        assertThat(queue.getString("name")).isEqualTo(queueName);
        assertThat(queue.getBoolean("auto_delete")).isEqualTo(queueAutoDelete);
        assertThat(queue.getBoolean("durable")).isEqualTo(queueDurable);
        assertThat(queue.getBoolean("exclusive")).isEqualTo(queueExclusive);
        assertThat(queue.getString("type")).isEqualTo(queueType);

        // verify bindings
        final JsonObject queueArguments = queue.getJsonObject("arguments");
        assertThat(queueArguments).isNotNull();
        assertThat(queueArguments.getString("x-dead-letter-exchange")).isNull();
        assertThat(queueArguments.getString("x-dead-letter-routing-key")).isNull();
        assertThat(queueArguments.getLong("x-message-ttl")).isEqualTo(queueTtl);
        assertThat(queueArguments.getString("x-queue-type")).isEqualTo(queueType);
        assertThat(queueArguments.getString("x-queue-mode")).isEqualTo(queueMode);
        assertThat(queueArguments.getBoolean("x-single-active-consumer")).isEqualTo(true);

        final JsonArray queueBindings = usage.getBindings(exchangeName, queueName);
        assertThat(queueBindings.size()).isEqualTo(2);

        final List<?> bindings = queueBindings.stream()
                .sorted(Comparator.comparing(x -> ((JsonObject) x).getString("routing_key")))
                .collect(Collectors.toList());

        final JsonObject binding1 = (JsonObject) bindings.get(0);
        assertThat(binding1).isNotNull();
        assertThat(binding1.getString("source")).isEqualTo(exchangeName);
        assertThat(binding1.getString("vhost")).isEqualTo("/");
        assertThat(binding1.getString("destination")).isEqualTo(queueName);
        assertThat(binding1.getString("destination_type")).isEqualTo("queue");
        assertThat(binding1.getString("routing_key")).isEqualTo("normal");

        final JsonObject binding2 = (JsonObject) bindings.get(1);
        assertThat(binding2).isNotNull();
        assertThat(binding2.getString("source")).isEqualTo(exchangeName);
        assertThat(binding2.getString("vhost")).isEqualTo("/");
        assertThat(binding2.getString("destination")).isEqualTo(queueName);
        assertThat(binding2.getString("destination_type")).isEqualTo("queue");
        assertThat(binding2.getString("routing_key")).isEqualTo("urgent");
    }

    /**
     * Verifies that Exchanges, Queues and Bindings are correctly declared as a result of
     * incoming connector configuration that specifies DLQ/DLX overrides.
     */
    @Test
    void testIncomingDeclarationsWithDLQ() throws Exception {
        final String exchangeName = "exchgIncomingDeclareTestWithDLQ";
        final boolean exchangeDurable = false;
        final boolean exchangeAutoDelete = true;
        final String exchangeType = "fanout";

        final String queueName = "qIncomingDeclareTestWithDLQ";
        final boolean queueDurable = false;
        final boolean queueExclusive = true;
        final boolean queueAutoDelete = true;
        final long queueTtl = 10000L;

        final String dlqName = "dlqIncomingDeclareTest";
        final String dlxName = "dlxIncomingDeclareTest";
        final String dlxType = "topic";
        final String dlxRoutingKey = "failure";
        final String dlqQueueType = "classic";
        final String dlqQueueMode = "default";

        final String routingKeys = "urgent, normal";

        weld.addBeanClass(IncomingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.exchange.durable", exchangeDurable)
                .put("mp.messaging.incoming.data.exchange.auto-delete", exchangeAutoDelete)
                .put("mp.messaging.incoming.data.exchange.type", exchangeType)
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.exchange.declare", true)
                .put("mp.messaging.incoming.data.queue.name", queueName)
                .put("mp.messaging.incoming.data.queue.durable", queueDurable)
                .put("mp.messaging.incoming.data.queue.exclusive", queueExclusive)
                .put("mp.messaging.incoming.data.queue.auto-delete", queueAutoDelete)
                .put("mp.messaging.incoming.data.queue.declare", true)
                .put("mp.messaging.incoming.data.queue.ttl", queueTtl)
                .put("mp.messaging.incoming.data.routing-keys", routingKeys)
                .put("mp.messaging.incoming.data.auto-bind-dlq", true)
                .put("mp.messaging.incoming.data.dead-letter-queue-name", dlqName)
                .put("mp.messaging.incoming.data.dead-letter-exchange", dlxName)
                .put("mp.messaging.incoming.data.dead-letter-exchange-type", dlxType)
                .put("mp.messaging.incoming.data.dead-letter-routing-key", dlxRoutingKey)
                .put("mp.messaging.incoming.data.dlx.declare", true)
                .put("mp.messaging.incoming.data.dead-letter-queue-type", dlqQueueType)
                .put("mp.messaging.incoming.data.dead-letter-queue-mode", dlqQueueMode)
                .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("mp.messaging.incoming.data.tracing.enabled", false)
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        // verify exchange
        final JsonObject exchange = usage.getExchange(exchangeName);
        assertThat(exchange).isNotNull();
        assertThat(exchange.getString("name")).isEqualTo(exchangeName);
        assertThat(exchange.getString("type")).isEqualTo(exchangeType);
        assertThat(exchange.getBoolean("auto_delete")).isEqualTo(exchangeAutoDelete);
        assertThat(exchange.getBoolean("durable")).isEqualTo(exchangeDurable);
        assertThat(exchange.getBoolean("internal")).isFalse();

        // verify dlx
        final JsonObject dlx = usage.getExchange(dlxName);
        assertThat(dlx).isNotNull();
        assertThat(dlx.getString("name")).isEqualTo(dlxName);
        assertThat(dlx.getString("type")).isEqualTo(dlxType);
        assertThat(dlx.getBoolean("auto_delete")).isFalse();
        assertThat(dlx.getBoolean("durable")).isTrue();
        assertThat(dlx.getBoolean("internal")).isFalse();

        // verify queue
        final JsonObject queue = usage.getQueue(queueName);
        assertThat(queue).isNotNull();
        assertThat(queue.getString("name")).isEqualTo(queueName);
        assertThat(queue.getBoolean("auto_delete")).isEqualTo(queueAutoDelete);
        assertThat(queue.getBoolean("durable")).isEqualTo(queueDurable);
        assertThat(queue.getBoolean("exclusive")).isEqualTo(queueExclusive);

        final JsonObject queueArguments = queue.getJsonObject("arguments");
        assertThat(queueArguments).isNotNull();
        assertThat(queueArguments.getString("x-dead-letter-exchange")).isEqualTo(dlxName);
        assertThat(queueArguments.getString("x-dead-letter-routing-key")).isEqualTo(dlxRoutingKey);
        assertThat(queueArguments.getLong("x-message-ttl")).isEqualTo(queueTtl);

        // verify dlq
        final JsonObject dlq = usage.getQueue(dlqName);
        assertThat(dlq).isNotNull();
        assertThat(dlq.getString("name")).isEqualTo(dlqName);
        assertThat(dlq.getBoolean("auto_delete")).isFalse();
        assertThat(dlq.getBoolean("durable")).isTrue();
        assertThat(dlq.getBoolean("exclusive")).isFalse();

        final JsonObject dlqArguments = dlq.getJsonObject("arguments");
        assertThat(dlqArguments.fieldNames()).isNotNull();
        assertThat(dlqArguments.getString("x-queue-type")).isEqualTo(dlqQueueType);
        assertThat(dlqArguments.getString("x-queue-mode")).isEqualTo(dlqQueueMode);

        // verify bindings
        final JsonArray queueBindings = usage.getBindings(exchangeName, queueName);
        assertThat(queueBindings.size()).isEqualTo(2);

        final List<?> bindings = queueBindings.stream()
                .sorted(Comparator.comparing(x -> ((JsonObject) x).getString("routing_key")))
                .collect(Collectors.toList());

        final JsonObject binding1 = (JsonObject) bindings.get(0);
        assertThat(binding1).isNotNull();
        assertThat(binding1.getString("source")).isEqualTo(exchangeName);
        assertThat(binding1.getString("vhost")).isEqualTo("/");
        assertThat(binding1.getString("destination")).isEqualTo(queueName);
        assertThat(binding1.getString("destination_type")).isEqualTo("queue");
        assertThat(binding1.getString("routing_key")).isEqualTo("normal");

        final JsonObject binding2 = (JsonObject) bindings.get(1);
        assertThat(binding2).isNotNull();
        assertThat(binding2.getString("source")).isEqualTo(exchangeName);
        assertThat(binding2.getString("vhost")).isEqualTo("/");
        assertThat(binding2.getString("destination")).isEqualTo(queueName);
        assertThat(binding2.getString("destination_type")).isEqualTo("queue");
        assertThat(binding2.getString("routing_key")).isEqualTo("urgent");

        // verify dlq bindings
        final JsonArray dlqBindings = usage.getBindings(dlxName, dlqName);
        assertThat(dlqBindings.size()).isEqualTo(1);

        final JsonObject dlqBinding1 = (JsonObject) dlqBindings.getJsonObject(0);
        assertThat(dlqBinding1).isNotNull();
        assertThat(dlqBinding1.getString("source")).isEqualTo(dlxName);
        assertThat(dlqBinding1.getString("vhost")).isEqualTo("/");
        assertThat(dlqBinding1.getString("destination")).isEqualTo(dlqName);
        assertThat(dlqBinding1.getString("destination_type")).isEqualTo("queue");
        assertThat(dlqBinding1.getString("routing_key")).isEqualTo(dlxRoutingKey);
    }

    /**
     * Verifies that messages can be sent to RabbitMQ.
     *
     * @throws InterruptedException
     */
    @Test
    void testSendingMessagesToRabbitMQ() throws InterruptedException {
        final String exchangeName = "exchg1";
        final String routingKey = "normal";

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers(exchangeName, routingKey, v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                .put("mp.messaging.outgoing.sink.exchange.declare", false)
                .put("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                .put("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.tracing.enabled", false)
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        assertThat(latch.await(3, TimeUnit.MINUTES)).isTrue();
    }

    /**
     * Verifies that messages can be sent to RabbitMQ.
     *
     * @throws InterruptedException
     */
    @Test
    void testSendingNullPayloadsToRabbitMQ() throws InterruptedException {
        final String exchangeName = "exchg1";
        final String routingKey = "normal";

        CountDownLatch latch = new CountDownLatch(10);
        usage.consume(exchangeName, routingKey, v -> latch.countDown());

        weld.addBeanClass(NullProducingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                .put("mp.messaging.outgoing.sink.exchange.declare", false)
                .put("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                .put("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.tracing.enabled", false)
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        assertThat(latch.await(3, TimeUnit.MINUTES)).isTrue();
    }

    /**
     * Verifies that messages can be received from RabbitMQ.
     */
    @Test
    void testReceivingMessagesFromRabbitMQ() {
        final String exchangeName = "exchg2";
        final String queueName = "q2";
        final String routingKey = "xyzzy";
        new MapBasedConfig()
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.exchange.durable", false)
                .put("mp.messaging.incoming.data.queue.name", queueName)
                .put("mp.messaging.incoming.data.queue.durable", false)
                .put("mp.messaging.incoming.data.queue.routing-keys", routingKey)
                .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("mp.messaging.incoming.data.tracing-enabled", false)
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        weld.addBeanClass(ConsumptionBean.class);

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));
        ConsumptionBean bean = container.getBeanManager().createInstance().select(ConsumptionBean.class).get();

        await().until(() -> isRabbitMQConnectorAvailable(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(exchangeName, queueName, routingKey, counter::getAndIncrement);

        await().atMost(1, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    /**
     * Verifies that messages can be received from RabbitMQ, but getPayload fails
     */
    @Test
    void testReceivingMessagesFromRabbitMQWithInvalidContentType() {
        final String exchangeName = "exchg3";
        final String queueName = "q3";
        final String routingKey = "xyzzy";
        new MapBasedConfig()
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.exchange.durable", false)
                .put("mp.messaging.incoming.data.queue.name", queueName)
                .put("mp.messaging.incoming.data.queue.durable", false)
                .put("mp.messaging.incoming.data.queue.routing-keys", routingKey)
                .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("mp.messaging.incoming.data.tracing-enabled", false)
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        weld.addBeanClass(ConsumptionBean.class);

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));
        ConsumptionBean bean = container.getBeanManager().createInstance().select(ConsumptionBean.class).get();

        await().until(() -> isRabbitMQConnectorAvailable(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produce(exchangeName, queueName, routingKey, 10, counter::getAndIncrement, "application/invalid");
        await().atMost(1, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(bean.getTypeCasts()).isEqualTo(10);
        assertThat(list).containsOnly(0);
    }

    /**
     * Verifies that message's content_type can be overridden
     */
    @Test
    void testReceivingMessagesFromRabbitMQWithOverriddenContentType() {
        final String exchangeName = "exchg4";
        final String queueName = "q4";
        final String routingKey = "xyzzy";
        new MapBasedConfig()
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.exchange.durable", false)
                .put("mp.messaging.incoming.data.queue.name", queueName)
                .put("mp.messaging.incoming.data.queue.durable", false)
                .put("mp.messaging.incoming.data.queue.routing-keys", routingKey)
                .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("mp.messaging.incoming.data.tracing-enabled", false)
                .put("mp.messaging.incoming.data.content-type-override", HttpHeaderValues.TEXT_PLAIN.toString())
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        weld.addBeanClass(ConsumptionBean.class);

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));
        ConsumptionBean bean = container.getBeanManager().createInstance().select(ConsumptionBean.class).get();

        await().until(() -> isRabbitMQConnectorAvailable(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produce(exchangeName, queueName, routingKey, 10, counter::getAndIncrement, "application/invalid");
        await().atMost(1, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(bean.getTypeCasts()).isEqualTo(0);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    /**
     * Verifies that default exchange name can be set with ("")
     */
    @Test
    void testDefaultExchangeName() {
        final String exchangeName = "\"\"";
        final String queueName = "q5";
        new MapBasedConfig()
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.queue.name", queueName)
                .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("mp.messaging.incoming.data.tracing-enabled", false)
                .put("mp.messaging.incoming.data.content-type-override", HttpHeaderValues.TEXT_PLAIN.toString())
                .put("rabbitmq-username", username)
                .put("rabbitmq-password", password)
                .put("rabbitmq-reconnect-attempts", 0)
                .write();

        weld.addBeanClass(ConsumptionBean.class);

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAvailable(container));
        ConsumptionBean bean = container.getBeanManager().createInstance().select(ConsumptionBean.class).get();

        await().until(() -> isRabbitMQConnectorAvailable(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produce("", queueName, queueName, 10, counter::getAndIncrement, "application/invalid");
        await().atMost(1, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(bean.getTypeCasts()).isEqualTo(0);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
}
