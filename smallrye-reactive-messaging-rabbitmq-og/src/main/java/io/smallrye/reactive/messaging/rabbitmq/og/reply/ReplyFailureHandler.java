package io.smallrye.reactive.messaging.rabbitmq.og.reply;

import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;

public interface ReplyFailureHandler {

    Throwable handleReply(IncomingRabbitMQMessage<?> replyMessage);
}
