package io.smallrye.reactive.messaging.kafka.reply;

import java.security.SecureRandom;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
@Identifier("bytes")
public class BytesCorrelationIdHandler implements CorrelationIdHandler {

    @Inject
    @ConfigProperty(name = "smallrye.kafka.request-reply.correlation-id.bytes.length", defaultValue = "12")
    int bytesLength;

    private final SecureRandom random = new SecureRandom();

    @Override
    public CorrelationId generate(Message<?> request) {
        byte[] bytes = new byte[bytesLength];
        random.nextBytes(bytes);
        return new BytesCorrelationId(bytes);
    }

    @Override
    public CorrelationId parse(byte[] bytes) {
        return new BytesCorrelationId(bytes);
    }
}
