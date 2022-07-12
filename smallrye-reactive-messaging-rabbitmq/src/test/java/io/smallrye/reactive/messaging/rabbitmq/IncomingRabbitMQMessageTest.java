package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;

import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
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
        io.vertx.rabbitmq.RabbitMQMessage mockMsg = mock(io.vertx.rabbitmq.RabbitMQMessage.class);
        when(mockMsg.body()).thenReturn(Buffer.buffer());
        when(mockMsg.properties()).thenReturn(new BasicProperties());
        when(mockMsg.envelope()).thenReturn(new Envelope(13456, false, "test", "test"));
        RabbitMQMessage msg = RabbitMQMessage.newInstance(mockMsg);

        Exception nackReason = new Exception("test");

        IncomingRabbitMQMessage<String> ackMsg = new IncomingRabbitMQMessage<>(msg, mock(ConnectionHolder.class), false,
                doNothingNack,
                doNothingAck,
                "text/plain");

        assertDoesNotThrow(() -> ackMsg.ack().toCompletableFuture().get());
        assertDoesNotThrow(() -> ackMsg.ack().toCompletableFuture().get());
        assertDoesNotThrow(() -> ackMsg.nack(nackReason).toCompletableFuture().get());
    }

    @Test
    public void testDoubleNackBehavior() {
        io.vertx.rabbitmq.RabbitMQMessage mockMsg = mock(io.vertx.rabbitmq.RabbitMQMessage.class);
        when(mockMsg.body()).thenReturn(Buffer.buffer());
        when(mockMsg.properties()).thenReturn(new BasicProperties());
        when(mockMsg.envelope()).thenReturn(new Envelope(13456, false, "test", "test"));
        RabbitMQMessage msg = RabbitMQMessage.newInstance(mockMsg);

        Exception nackReason = new Exception("test");

        IncomingRabbitMQMessage<String> nackMsg = new IncomingRabbitMQMessage<>(msg, mock(ConnectionHolder.class), false,
                doNothingNack,
                doNothingAck,
                "text/plain");

        assertDoesNotThrow(() -> nackMsg.nack(nackReason).toCompletableFuture().get());
        assertDoesNotThrow(() -> nackMsg.nack(nackReason).toCompletableFuture().get());
        assertDoesNotThrow(() -> nackMsg.ack().toCompletableFuture().get());
    }

    @Test
    void testConvertPayload() {
        io.vertx.rabbitmq.RabbitMQMessage mockMsg = mock(io.vertx.rabbitmq.RabbitMQMessage.class);
        when(mockMsg.body()).thenReturn(Buffer.buffer("payload"));
        when(mockMsg.properties()).thenReturn(new BasicProperties());
        when(mockMsg.envelope()).thenReturn(new Envelope(13456, false, "test", "test"));
        RabbitMQMessage msg = RabbitMQMessage.newInstance(mockMsg);

        IncomingRabbitMQMessage<String> incomingRabbitMQMessage = new IncomingRabbitMQMessage<>(msg,
                mock(ConnectionHolder.class), false,
                doNothingNack, doNothingAck, "text/plain");

        assertThat(incomingRabbitMQMessage.getPayload()).isEqualTo("payload");
    }

    @Test
    void testConvertPayloadJsonObject() {
        io.vertx.rabbitmq.RabbitMQMessage mockMsg = mock(io.vertx.rabbitmq.RabbitMQMessage.class);
        JsonObject payload = new JsonObject();
        payload.putNull("key");
        when(mockMsg.body()).thenReturn(payload.toBuffer());
        when(mockMsg.properties()).thenReturn(new BasicProperties.Builder().contentType("application/json").build());
        when(mockMsg.envelope()).thenReturn(new Envelope(13456, false, "test", "test"));
        RabbitMQMessage msg = RabbitMQMessage.newInstance(mockMsg);

        IncomingRabbitMQMessage<JsonObject> incomingRabbitMQMessage = new IncomingRabbitMQMessage<>(msg,
                mock(ConnectionHolder.class), false,
                doNothingNack, doNothingAck, null);

        assertThat(incomingRabbitMQMessage.getPayload()).isEqualTo(payload);
    }

    @Test
    void testConvertPayloadFallback() {
        io.vertx.rabbitmq.RabbitMQMessage mockMsg = mock(io.vertx.rabbitmq.RabbitMQMessage.class);
        Buffer payloadBuffer = Buffer.buffer("payload");
        when(mockMsg.body()).thenReturn(payloadBuffer);
        when(mockMsg.properties()).thenReturn(new BasicProperties.Builder().contentType("application/json").build());
        when(mockMsg.envelope()).thenReturn(new Envelope(13456, false, "test", "test"));
        RabbitMQMessage msg = RabbitMQMessage.newInstance(mockMsg);

        IncomingRabbitMQMessage<JsonObject> incomingRabbitMQMessage = new IncomingRabbitMQMessage<>(msg,
                mock(ConnectionHolder.class), false,
                doNothingNack, doNothingAck, null);

        assertThat(((Message<byte[]>) ((Message) incomingRabbitMQMessage)).getPayload()).isEqualTo(payloadBuffer.getBytes());
    }
}
