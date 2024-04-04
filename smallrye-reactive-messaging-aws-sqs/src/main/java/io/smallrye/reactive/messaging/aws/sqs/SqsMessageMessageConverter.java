package io.smallrye.reactive.messaging.aws.sqs;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

@ApplicationScoped
public class SqsMessageMessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.isAssignable(target, software.amazon.awssdk.services.sqs.model.Message.class)
                && in.getMetadata(SqsIncomingMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(in.getMetadata(SqsIncomingMetadata.class)
                .map(SqsIncomingMetadata::getMessage)
                .orElse(null));
    }
}
