package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * CDI/Framework integration tests for the RabbitMQ OG connector.
 * Tests use @Incoming, @Outgoing, and @Channel annotations with full Weld/CDI setup.
 */
@SuppressWarnings("ConstantConditions")
class RabbitMQIntegrationTest extends WeldTestBase {

    /**
     * Verifies that Exchanges are correctly declared as a result of outgoing connector
     * configuration.
     */
    @Test
    void testOutgoingDeclarations() throws Exception {

        final boolean exchangeDurable = false;
        final boolean exchangeAutoDelete = true;
        final String exchangeType = "fanout";

        weld.addBeanClass(OutgoingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                .put("mp.messaging.outgoing.sink.exchange.durable", exchangeDurable)
                .put("mp.messaging.outgoing.sink.exchange.auto-delete", exchangeAutoDelete)
                .put("mp.messaging.outgoing.sink.exchange.type", exchangeType)
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
        final boolean exchangeDurable = false;
        final boolean exchangeAutoDelete = true;
        final String exchangeType = "fanout";

        final boolean queueDurable = false;
        final boolean queueExclusive = true;
        final boolean queueAutoDelete = true;
        final long queueTtl = 10000L;
        final String queueType = "classic";
        final String queueMode = "default";

        final String routingKeys = "urgent, normal";
        final String arguments = "key1:value1,key2:value2";

        weld.addBeanClass(IncomingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                .put("mp.messaging.incoming.data.exchange.durable", exchangeDurable)
                .put("mp.messaging.incoming.data.exchange.auto-delete", exchangeAutoDelete)
                .put("mp.messaging.incoming.data.exchange.type", exchangeType)
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
                .put("mp.messaging.incoming.data.arguments", arguments)
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

        // verify queue arguments
        final JsonObject queueArguments = queue.getJsonObject("arguments");
        assertThat(queueArguments).isNotNull();
        assertThat(queueArguments.getString("x-dead-letter-exchange")).isNull();
        assertThat(queueArguments.getString("x-dead-letter-routing-key")).isNull();
        assertThat(queueArguments.getLong("x-message-ttl")).isEqualTo(queueTtl);
        assertThat(queueArguments.getString("x-queue-type")).isEqualTo(queueType);
        assertThat(queueArguments.getString("x-queue-mode")).isEqualTo(queueMode);
        assertThat(queueArguments.getBoolean("x-single-active-consumer")).isEqualTo(true);

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

        final JsonObject binding1Arguments = binding1.getJsonObject("arguments");
        assertThat(binding1Arguments.getString("key1")).isEqualTo("value1");
        assertThat(binding1Arguments.getString("key2")).isEqualTo("value2");

        final JsonObject binding2 = (JsonObject) bindings.get(1);
        assertThat(binding2).isNotNull();
        assertThat(binding2.getString("source")).isEqualTo(exchangeName);
        assertThat(binding2.getString("vhost")).isEqualTo("/");
        assertThat(binding2.getString("destination")).isEqualTo(queueName);
        assertThat(binding2.getString("destination_type")).isEqualTo("queue");
        assertThat(binding2.getString("routing_key")).isEqualTo("urgent");
    }

    /**
     * Verifies that messages can be sent to and received from RabbitMQ using @Incoming/@Outgoing.
     */
    @Test
    void testBasicMessaging() throws InterruptedException {
        final String routingKey = "normal";

        CountDownLatch latch = new CountDownLatch(10);
        usage.consume(exchangeName, routingKey, v -> latch.countDown());

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

        // Wait for messages to be produced and consumed
        assertThat(latch.await(2, TimeUnit.MINUTES)).isTrue();
    }

}
