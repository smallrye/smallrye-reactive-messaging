package io.smallrye.reactive.messaging.inject;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageKeyValueExtractor;

@ApplicationScoped
public class PayloadKeyValueExtractor implements MessageKeyValueExtractor {

    @Override
    public boolean canExtract(Message<?> in, Type keyType, Type valueType) {
        return true;
    }

    @Override
    public Object extractKey(Message<?> in, Type target) {
        return in.getPayload();
    }

    @Override
    public Object extractValue(Message<?> in, Type target) {
        return in.getPayload();
    }
}
