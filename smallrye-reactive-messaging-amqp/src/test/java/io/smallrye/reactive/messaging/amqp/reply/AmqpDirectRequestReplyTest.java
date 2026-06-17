package io.smallrye.reactive.messaging.amqp.reply;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.amqp.AmqpBrokerTestBase;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.mutiny.amqp.AmqpSender;

public class AmqpDirectRequestReplyTest extends AmqpBrokerTestBase {

    @Test
    void testRequestReply() {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String requestAddress = "test-request-" + id;
        String replyAddress = requestAddress + "-client-reply-to";

        AmqpConnection amqpConnection = usage.client.connectAndAwait();
        AmqpSender replySender = usage.client.createSender(replyAddress).await().indefinitely();
        usage.client.createReceiver(requestAddress)
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .onItem().transformToUniAndConcatenate(request -> replySender.sendWithAck(AmqpMessage.create()
                        .correlationId(request.id())
                        .address(request.replyTo())
                        .withBody("pong")
                        .build()))
                .subscribe().with(x -> {

                });

        AmqpDirectRequestReply amqpRequestReply = new AmqpDirectRequestReply(amqpConnection, requestAddress,
                "test-link-" + id);
        String replyBody = amqpRequestReply.request(AmqpMessage.create().withBody("ping").build())
                .await().atMost(Duration.ofSeconds(4))
                .bodyAsString();
        assertThat(replyBody).isEqualTo("pong");
        amqpRequestReply.close();
    }

    @Test
    void testRequestTimeout() {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String requestAddress = "test-request-" + id;

        AmqpConnection amqpConnection = usage.client.connectAndAwait();

        AmqpDirectRequestReply amqpRequestReply = new AmqpDirectRequestReply(amqpConnection, requestAddress,
                "test-link-" + id);
        Assertions.assertThatThrownBy(() -> {
            amqpRequestReply.request(AmqpMessage.create().withBody("ping").build())
                    .await().atMost(Duration.ofSeconds(4));
        }).isInstanceOf(AmqpRequestReplyTimeoutException.class);
        amqpRequestReply.close();
    }
}
