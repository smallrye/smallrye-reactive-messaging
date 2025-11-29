package io.smallrye.reactive.messaging.rabbitmq.converter;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.mutiny.core.buffer.Buffer;

@ApplicationScoped
public class TypeMessageConverter implements MessageConverter {

    private final Instance<JsonMapping> jsonMapping;

    @Inject
    public TypeMessageConverter(Instance<JsonMapping> jsonMapping) {
        this.jsonMapping = jsonMapping;
    }

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return jsonMapping.isResolvable()
                && in instanceof IncomingRabbitMQMessage<?>
                && ((IncomingRabbitMQMessage<?>) in).getContentEncoding().isEmpty()
                && ((IncomingRabbitMQMessage<?>) in).getEffectiveContentType()
                        .map(contentType -> HttpHeaderValues.APPLICATION_JSON.toString().equalsIgnoreCase(contentType))
                        .orElse(false);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(
                jsonMapping.get().fromJson(Buffer.buffer((byte[]) in.getPayload()).toString(), target));
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

}
