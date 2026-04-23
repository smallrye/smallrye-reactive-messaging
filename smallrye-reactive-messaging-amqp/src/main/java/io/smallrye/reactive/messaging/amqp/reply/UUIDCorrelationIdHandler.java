package io.smallrye.reactive.messaging.amqp.reply;

import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
@Identifier("uuid")
public class UUIDCorrelationIdHandler implements CorrelationIdHandler {
    @Override
    public CorrelationId generate(Message<?> request) {
        return new StringCorrelationId(UUID.randomUUID().toString());
    }

    @Override
    public CorrelationId parse(String string) {
        return new StringCorrelationId(string);
    }

}
