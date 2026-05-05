package io.smallrye.reactive.messaging.rabbitmq.og.reply;

import org.eclipse.microprofile.reactive.messaging.Message;

public interface CorrelationIdHandler {

    CorrelationId generate(Message<?> request);

    CorrelationId parse(String string);
}
