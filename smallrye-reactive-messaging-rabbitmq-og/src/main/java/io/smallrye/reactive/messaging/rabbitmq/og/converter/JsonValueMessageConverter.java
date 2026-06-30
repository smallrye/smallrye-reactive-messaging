package io.smallrye.reactive.messaging.rabbitmq.og.converter;

import java.lang.reflect.Type;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

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
        String encoding = metadata.getContentEncoding();
        return (encoding == null || encoding.isEmpty())
                && metadata.getEffectiveContentType()
                        .map(contentType -> "application/json".equalsIgnoreCase(contentType))
                        .orElse(false);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingRabbitMQMetadata metadata = in.getMetadata(IncomingRabbitMQMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No RabbitMQ metadata"));
        byte[] body = metadata.getBody();
        return in.withPayload(Json.decodeValue(Buffer.buffer(body)));
    }
}
