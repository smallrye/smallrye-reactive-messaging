package io.smallrye.reactive.messaging.rabbitmq.og.converter;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata;

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
        return in.withPayload(jsonMapping.get().fromJson(new String(body, StandardCharsets.UTF_8), target));
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }
}
