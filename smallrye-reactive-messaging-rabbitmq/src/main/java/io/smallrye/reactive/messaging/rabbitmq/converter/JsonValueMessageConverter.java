package io.smallrye.reactive.messaging.rabbitmq.converter;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

@ApplicationScoped
public class JsonValueMessageConverter implements MessageConverter {

    private final static List<Type> JSON_VALUE_TYPES = Arrays.asList(JsonObject.class, JsonArray.class, String.class);

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in instanceof IncomingRabbitMQMessage<?>
                && ((IncomingRabbitMQMessage<?>) in).getContentEncoding().isEmpty()
                && ((IncomingRabbitMQMessage<?>) in).getEffectiveContentType()
                        .map(contentType -> HttpHeaderValues.APPLICATION_JSON.toString().equalsIgnoreCase(contentType))
                        .orElse(false)
                && JSON_VALUE_TYPES.contains(target);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(Buffer.buffer((byte[]) in.getPayload()).toJsonValue());
    }
}
