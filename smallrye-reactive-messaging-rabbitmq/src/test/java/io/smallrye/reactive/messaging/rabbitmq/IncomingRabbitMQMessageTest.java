package io.smallrye.reactive.messaging.rabbitmq;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;

import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.rabbitmq.RabbitMQMessage;

public class IncomingRabbitMQMessageTest {

    RabbitMQAckHandler doNothingAck = new RabbitMQAckHandler() {
        @Override
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Context context) {
            return CompletableFuture.completedFuture(null);
        }
    };

    RabbitMQFailureHandler doNothingNack = new RabbitMQFailureHandler() {
        @Override
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Context context, Throwable reason) {
            return CompletableFuture.completedFuture(null);
        }
    };

    @Test
    public void testDoubleAckBehavior() {

        RabbitMQMessage msg = RabbitMQMessage.newInstance(mock(io.vertx.rabbitmq.RabbitMQMessage.class));
        when(msg.properties()).thenReturn(new BasicProperties());
        when(msg.envelope()).thenReturn(new Envelope(13456, false, "test", "test"));

        Exception nackReason = new Exception("test");

        IncomingRabbitMQMessage<Buffer> ackMsg = IncomingRabbitMQMessage.create(msg, mock(ConnectionHolder.class), false,
                doNothingNack,
                doNothingAck);

        assertDoesNotThrow(() -> ackMsg.ack().toCompletableFuture().get());
        assertDoesNotThrow(() -> ackMsg.ack().toCompletableFuture().get());
        assertDoesNotThrow(() -> ackMsg.nack(nackReason).toCompletableFuture().get());
    }

    @Test
    public void testDoubleNackBehavior() {

        RabbitMQMessage msg = RabbitMQMessage.newInstance(mock(io.vertx.rabbitmq.RabbitMQMessage.class));
        when(msg.properties()).thenReturn(new BasicProperties());
        when(msg.envelope()).thenReturn(new Envelope(13456, false, "test", "test"));

        Exception nackReason = new Exception("test");

        IncomingRabbitMQMessage<Buffer> nackMsg = IncomingRabbitMQMessage.create(msg, mock(ConnectionHolder.class), false,
                doNothingNack,
                doNothingAck);

        assertDoesNotThrow(() -> nackMsg.nack(nackReason).toCompletableFuture().get());
        assertDoesNotThrow(() -> nackMsg.nack(nackReason).toCompletableFuture().get());
        assertDoesNotThrow(() -> nackMsg.ack().toCompletableFuture().get());
    }
}
