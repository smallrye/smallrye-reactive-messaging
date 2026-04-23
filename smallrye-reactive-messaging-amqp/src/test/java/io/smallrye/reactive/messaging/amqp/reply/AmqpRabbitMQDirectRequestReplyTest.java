package io.smallrye.reactive.messaging.amqp.reply;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.amqp.RabbitMQBrokerTestBase;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.mutiny.amqp.AmqpSender;

public class AmqpRabbitMQDirectRequestReplyTest extends RabbitMQBrokerTestBase {

    @Test
    void testRequestReply() {
        AmqpConnection amqpConnection = usage.client.connectAndAwait();
        AmqpSender replySender = usage.client.createSender("test-request-client-reply-to").await().indefinitely();
        usage.client.createReceiver("test-request")
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .onItem().transformToUniAndConcatenate(request -> replySender.sendWithAck(AmqpMessage.create()
                        .correlationId(request.id())
                        .address(request.replyTo())
                        .withBody("pong")
                        .build()))
                .subscribe().with(x -> {

                });

        AmqpDirectRequestReply amqpRequestReply = new AmqpDirectRequestReply(amqpConnection, "test-request", "test-link");
        String replyBody = amqpRequestReply.request(AmqpMessage.create().withBody("ping").build())
                .await().atMost(Duration.ofSeconds(4))
                .bodyAsString();
        assertThat(replyBody).isEqualTo("pong");
        amqpRequestReply.close();
    }

    @Test
    void testRequestTimeout() {
        AmqpConnection amqpConnection = usage.client.connectAndAwait();
        // no reply sender is created, so the request should timeout

        AmqpDirectRequestReply amqpRequestReply = new AmqpDirectRequestReply(amqpConnection, "test-request", "test-link");
        Assertions.assertThatThrownBy(() -> {
            amqpRequestReply.request(AmqpMessage.create().withBody("ping").build())
                    .await().atMost(Duration.ofSeconds(4));
        }).isInstanceOf(AmqpRequestReplyTimeoutException.class);
        amqpRequestReply.close();
    }
}
