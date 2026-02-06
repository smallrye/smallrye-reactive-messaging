package io.smallrye.reactive.messaging.rabbitmq.converter;

import java.lang.reflect.Type;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.vertx.rabbitmq.RabbitMQMessage;

@ApplicationScoped
public class TypeMessageConverter implements MessageConverter {

    private final Instance<JsonMapping> jsonMapping;

    @Inject
    public TypeMessageConverter(Instance<JsonMapping> jsonMapping) {
        this.jsonMapping = jsonMapping;
    }

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        if (!jsonMapping.isResolvable()) {
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
        return in.withPayload(jsonMapping.get().fromJson(message.body().toString(), target));
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

}
