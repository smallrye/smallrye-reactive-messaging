package io.smallrye.reactive.messaging.rabbitmq.converter;

import java.lang.reflect.Type;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQMessage;

@ApplicationScoped
public class JsonValueMessageConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        if (!(String.class.equals(target) || JsonObject.class.equals(target) || JsonArray.class.equals(target))) {
            return false;
        }
        Optional<IncomingRabbitMQMetadata> maybe = in.getMetadata(IncomingRabbitMQMetadata.class);
        if (maybe.isEmpty()) {
            return false;
        }
        IncomingRabbitMQMetadata metadata = maybe.get();
        return metadata.getContentEncoding().isEmpty()
                && metadata.getEffectiveContentType()
                        .map(contentType -> HttpHeaderValues.APPLICATION_JSON.toString().equalsIgnoreCase(contentType))
                        .orElse(false);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingRabbitMQMetadata metadata = in.getMetadata(IncomingRabbitMQMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No RabbitMQ metadata"));
        RabbitMQMessage message = metadata.getMessage()
                .orElseThrow(() -> new IllegalStateException("No RabbitMQ message"));
        return in.withPayload(message.body().toJsonValue());
    }
}
