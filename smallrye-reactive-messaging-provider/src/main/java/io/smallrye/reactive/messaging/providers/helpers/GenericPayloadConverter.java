package io.smallrye.reactive.messaging.providers.helpers;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.GenericPayload;
import io.smallrye.reactive.messaging.MessageConverter;

@ApplicationScoped
public class GenericPayloadConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.getRawTypeIfParameterized(target).equals(GenericPayload.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(GenericPayload.from(in));
    }
}
