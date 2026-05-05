package io.smallrye.reactive.messaging.rabbitmq.og.reply;

import io.smallrye.reactive.messaging.rabbitmq.og.OutgoingRabbitMQMetadata;

public interface PendingReply {

    OutgoingRabbitMQMetadata metadata();

    void complete();

    boolean isCancelled();
}
