package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;

import io.smallrye.reactive.messaging.rabbitmq.og.fault.RabbitMQAccept;
import io.smallrye.reactive.messaging.rabbitmq.og.fault.RabbitMQFailStop;
import io.smallrye.reactive.messaging.rabbitmq.og.fault.RabbitMQFailureHandler;
import io.smallrye.reactive.messaging.rabbitmq.og.fault.RabbitMQReject;
import io.smallrye.reactive.messaging.rabbitmq.og.fault.RabbitMQRequeue;
import io.vertx.mutiny.core.Vertx;

/**
 * Tests for failure handling strategies
 */
public class FailureHandlerTest extends RabbitMQBrokerTestBase {

    @Test
    public void testAcceptStrategy() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            io.vertx.core.Context context = holder.getContext();

            // Create test message
            Envelope envelope = new Envelope(1L, false, exchangeName, "test.key");
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .build();
            byte[] body = "Test message".getBytes(StandardCharsets.UTF_8);

            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck ackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck(
                    channel, context);
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack nackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack(
                    channel, context, false);

            IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                    envelope,
                    props,
                    body,
                    IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                    ackHandler,
                    nackHandler);

            // Create accept strategy
            RabbitMQAccept acceptStrategy = new RabbitMQAccept("test-channel");

            CountDownLatch latch = new CountDownLatch(1);
            Throwable testError = new RuntimeException("Test error");

            // Handle failure - should accept (ack) the message
            acceptStrategy.handle(message, null, io.vertx.mutiny.core.Context.newInstance(context), testError)
                    .whenComplete((v, t) -> {
                        assertThat(t).isNull(); // Should succeed
                        latch.countDown();
                    });

            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testRejectStrategy() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            io.vertx.core.Context context = holder.getContext();

            // Create test message
            Envelope envelope = new Envelope(1L, false, exchangeName, "test.key");
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .build();
            byte[] body = "Test message".getBytes(StandardCharsets.UTF_8);

            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck ackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck(
                    channel, context);
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack nackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack(
                    channel, context, false);

            IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                    envelope,
                    props,
                    body,
                    IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                    ackHandler,
                    nackHandler);

            // Create reject strategy
            RabbitMQReject rejectStrategy = new RabbitMQReject("test-channel");

            CountDownLatch latch = new CountDownLatch(1);
            Throwable testError = new RuntimeException("Test error");

            // Handle failure - should reject (nack without requeue) the message
            rejectStrategy.handle(message, null, io.vertx.mutiny.core.Context.newInstance(context), testError)
                    .whenComplete((v, t) -> {
                        assertThat(t).isNull(); // Should succeed
                        latch.countDown();
                    });

            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testRequeueStrategy() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            io.vertx.core.Context context = holder.getContext();

            // Create test message
            Envelope envelope = new Envelope(1L, false, exchangeName, "test.key");
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .build();
            byte[] body = "Test message".getBytes(StandardCharsets.UTF_8);

            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck ackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck(
                    channel, context);
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack nackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack(
                    channel, context, true);

            IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                    envelope,
                    props,
                    body,
                    IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                    ackHandler,
                    nackHandler);

            // Create requeue strategy
            RabbitMQRequeue requeueStrategy = new RabbitMQRequeue("test-channel");

            CountDownLatch latch = new CountDownLatch(1);
            Throwable testError = new RuntimeException("Test error");

            // Handle failure - should nack with requeue
            requeueStrategy.handle(message, null, io.vertx.mutiny.core.Context.newInstance(context), testError)
                    .whenComplete((v, t) -> {
                        assertThat(t).isNull(); // Should succeed
                        latch.countDown();
                    });

            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testRequeueWithCustomMetadata() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            io.vertx.core.Context context = holder.getContext();

            // Create test message
            Envelope envelope = new Envelope(1L, false, exchangeName, "test.key");
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .build();
            byte[] body = "Test message".getBytes(StandardCharsets.UTF_8);

            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck ackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck(
                    channel, context);
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack nackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack(
                    channel, context, true);

            IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                    envelope,
                    props,
                    body,
                    IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                    ackHandler,
                    nackHandler);

            // Create requeue strategy
            RabbitMQRequeue requeueStrategy = new RabbitMQRequeue("test-channel");

            CountDownLatch latch = new CountDownLatch(1);
            Throwable testError = new RuntimeException("Test error");

            // Metadata with requeue=false should override default
            Metadata metadata = Metadata.of(new RabbitMQRejectMetadata(false));

            requeueStrategy.handle(message, metadata, io.vertx.mutiny.core.Context.newInstance(context), testError)
                    .whenComplete((v, t) -> {
                        assertThat(t).isNull();
                        latch.countDown();
                    });

            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testFailStopStrategy() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            io.vertx.core.Context context = holder.getContext();

            // Create test message
            Envelope envelope = new Envelope(1L, false, exchangeName, "test.key");
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .build();
            byte[] body = "Test message".getBytes(StandardCharsets.UTF_8);

            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck ackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck(
                    channel, context);
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack nackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack(
                    channel, context, false);

            IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                    envelope,
                    props,
                    body,
                    IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                    ackHandler,
                    nackHandler);

            // Create fail-stop strategy
            RabbitMQFailStop failStopStrategy = new RabbitMQFailStop("test-channel");

            CountDownLatch latch = new CountDownLatch(1);
            RuntimeException testError = new RuntimeException("Test error");
            AtomicReference<Throwable> capturedError = new AtomicReference<>();

            // Handle failure - should nack and then fail
            failStopStrategy.handle(message, null, io.vertx.mutiny.core.Context.newInstance(context), testError)
                    .whenComplete((v, t) -> {
                        capturedError.set(t);
                        latch.countDown();
                    });

            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(capturedError.get()).isNotNull();
            assertThat(capturedError.get()).isInstanceOf(RuntimeException.class);
            assertThat(capturedError.get().getMessage()).isEqualTo("Test error");

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testRequeueStrategyActuallyRequeues() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            io.vertx.core.Context context = holder.getContext();

            // Declare a temporary queue
            String testQueue = queueName + "-requeue-test";
            channel.queueDeclare(testQueue, false, false, true, null);

            // Publish a message
            byte[] body = "Requeue test".getBytes(StandardCharsets.UTF_8);
            channel.basicPublish("", testQueue, null, body);

            // Consume the message with manual ack
            com.rabbitmq.client.GetResponse response = channel.basicGet(testQueue, false);
            assertThat(response).isNotNull();

            // Create handlers - nack handler with requeue=true (requeue strategy default)
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck ackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck(
                    channel, context);
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack nackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack(
                    channel, context, true);

            IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                    response.getEnvelope(),
                    response.getProps(),
                    response.getBody(),
                    IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                    ackHandler,
                    nackHandler);

            // Nack with requeue via handler
            CountDownLatch latch = new CountDownLatch(1);
            nackHandler.handle(message, null, new RuntimeException("test"))
                    .whenComplete((v, t) -> latch.countDown());
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

            // The message should reappear in the queue as redelivered
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                com.rabbitmq.client.GetResponse redelivered = channel.basicGet(testQueue, true);
                assertThat(redelivered).isNotNull();
                assertThat(new String(redelivered.getBody(), StandardCharsets.UTF_8)).isEqualTo("Requeue test");
                assertThat(redelivered.getEnvelope().isRedeliver()).isTrue();
            });

            channel.queueDelete(testQueue);
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testRejectStrategyDoesNotRequeue() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            io.vertx.core.Context context = holder.getContext();

            // Declare a temporary queue
            String testQueue = queueName + "-reject-test";
            channel.queueDeclare(testQueue, false, false, true, null);

            // Publish a message
            byte[] body = "Reject test".getBytes(StandardCharsets.UTF_8);
            channel.basicPublish("", testQueue, null, body);

            // Consume the message with manual ack
            com.rabbitmq.client.GetResponse response = channel.basicGet(testQueue, false);
            assertThat(response).isNotNull();

            // Create handlers - nack handler with requeue=false (reject strategy default)
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck ackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck(
                    channel, context);
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack nackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack(
                    channel, context, false);

            IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                    response.getEnvelope(),
                    response.getProps(),
                    response.getBody(),
                    IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                    ackHandler,
                    nackHandler);

            // Nack with requeue=false via reject metadata
            CountDownLatch latch = new CountDownLatch(1);
            Metadata rejectMetadata = Metadata.of(new RabbitMQRejectMetadata(false));
            nackHandler.handle(message, rejectMetadata, new RuntimeException("test"))
                    .whenComplete((v, t) -> latch.countDown());
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

            // The message should NOT reappear in the queue
            Thread.sleep(500);
            com.rabbitmq.client.GetResponse gone = channel.basicGet(testQueue, true);
            assertThat(gone).isNull();

            channel.queueDelete(testQueue);
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testRequeueWithMetadataOverrideFalse() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            io.vertx.core.Context context = holder.getContext();

            // Declare a temporary queue
            String testQueue = queueName + "-metadata-override-test";
            channel.queueDeclare(testQueue, false, false, true, null);

            // Publish a message
            byte[] body = "Metadata override test".getBytes(StandardCharsets.UTF_8);
            channel.basicPublish("", testQueue, null, body);

            // Consume the message with manual ack
            com.rabbitmq.client.GetResponse response = channel.basicGet(testQueue, false);
            assertThat(response).isNotNull();

            // Create handlers - nack handler with requeue=true (requeue strategy default)
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck ackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck(
                    channel, context);
            io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack nackHandler = new io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack(
                    channel, context, true);

            IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                    response.getEnvelope(),
                    response.getProps(),
                    response.getBody(),
                    IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                    ackHandler,
                    nackHandler);

            // Nack with metadata override: requeue=false despite handler default of true
            CountDownLatch latch = new CountDownLatch(1);
            Metadata overrideMetadata = Metadata.of(new RabbitMQRejectMetadata(false));
            nackHandler.handle(message, overrideMetadata, new RuntimeException("test"))
                    .whenComplete((v, t) -> latch.countDown());
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

            // The message should NOT reappear because metadata override set requeue=false
            Thread.sleep(500);
            com.rabbitmq.client.GetResponse gone = channel.basicGet(testQueue, true);
            assertThat(gone).isNull();

            channel.queueDelete(testQueue);
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testFactories() {
        // Test that factories create correct strategies
        TestIncomingConfig config = new TestIncomingConfig();

        RabbitMQFailStop.Factory failStopFactory = new RabbitMQFailStop.Factory();
        RabbitMQFailureHandler failStopHandler = failStopFactory.create(config, null);
        assertThat(failStopHandler).isInstanceOf(RabbitMQFailStop.class);

        RabbitMQAccept.Factory acceptFactory = new RabbitMQAccept.Factory();
        RabbitMQFailureHandler acceptHandler = acceptFactory.create(config, null);
        assertThat(acceptHandler).isInstanceOf(RabbitMQAccept.class);

        RabbitMQReject.Factory rejectFactory = new RabbitMQReject.Factory();
        RabbitMQFailureHandler rejectHandler = rejectFactory.create(config, null);
        assertThat(rejectHandler).isInstanceOf(RabbitMQReject.class);

        RabbitMQRequeue.Factory requeueFactory = new RabbitMQRequeue.Factory();
        RabbitMQFailureHandler requeueHandler = requeueFactory.create(config, null);
        assertThat(requeueHandler).isInstanceOf(RabbitMQRequeue.class);
    }

    // Simple test configuration
    static class TestIncomingConfig extends RabbitMQConnectorIncomingConfiguration {
        public TestIncomingConfig() {
            super(null);
        }

        @Override
        public String getChannel() {
            return "test-channel";
        }
    }
}
