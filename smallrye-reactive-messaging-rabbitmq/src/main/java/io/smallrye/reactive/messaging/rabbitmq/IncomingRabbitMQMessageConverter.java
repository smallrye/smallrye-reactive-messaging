package io.smallrye.reactive.messaging.rabbitmq;

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;

import java.lang.reflect.Type;
import java.util.Optional;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.IncomingMessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public abstract class IncomingRabbitMQMessageConverter implements IncomingMessageConverter {

    @Override
    public int getPriority() {
        return -1;
    }

    abstract boolean canConvert(IncomingRabbitMQMessage<Buffer> in, Type target);

    abstract Message<?> convert(IncomingRabbitMQMessage<Buffer> in);

    @SuppressWarnings("unchecked")
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        if (!(in instanceof IncomingRabbitMQMessage<?>) || !(in.getPayload() instanceof Buffer)) {
            return false;
        }
        return canConvert((IncomingRabbitMQMessage<Buffer>) in, target);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Uni<Message<?>> convert(Message<?> in, Type target) {
        Uni<? extends Message<?>> stream = Uni.createFrom().nullItem()
                .map(v -> convert((IncomingRabbitMQMessage<Buffer>) in))
                .onFailure().call(t -> Uni.createFrom().completionStage(in.nack(t)))
                .onFailure().invoke(RabbitMQLogging.log::payloadConversionFailed);
        return (Uni<Message<?>>) stream;
    }

    @Singleton
    public static class ByteArrayPayloadConverter extends IncomingRabbitMQMessageConverter {

        @Override
        public boolean canConvert(IncomingRabbitMQMessage<Buffer> in, Type target) {
            // We only do payload conversions to byte[]
            return target == byte[].class;
        }

        @Override
        public Message<?> convert(IncomingRabbitMQMessage<Buffer> in) {
            return in.withPayload(in.getPayload().getBytes());
        }
    }

    @Singleton
    public static class TextConverter extends IncomingRabbitMQMessageConverter {

        boolean canConvert(IncomingRabbitMQMessage<Buffer> in, Type target) {
            // Ensure content-type is text
            Optional<String> contentType = in.getContentType();
            // TODO use proper MIME type comparison
            if (!contentType.orElse("").toLowerCase().startsWith("text/")) {
                return false;
            }

            // We only do payload conversions to text
            return target == String.class;
        }

        @Override
        public Message<?> convert(IncomingRabbitMQMessage<Buffer> in) {
            // TODO respect encoding
            return in.withPayload(in.getPayload().toString());
        }
    }

    @Singleton
    public static class JSONConverter extends IncomingRabbitMQMessageConverter {

        boolean canConvert(IncomingRabbitMQMessage<Buffer> in, Type target) {
            // Ensure content-type is JSON
            Optional<String> contentType = in.getContentType();
            // TODO use proper MIME type comparison
            if (!contentType.orElse("").equalsIgnoreCase(APPLICATION_JSON.toString())) {
                return false;
            }

            // We only do payload conversions to JSON
            return target == JsonObject.class ||
                    target == JsonArray.class ||
                    target == String.class ||
                    target == Boolean.class ||
                    TypeUtils.isAssignable(target, Number.class);
        }

        @Override
        public Message<?> convert(IncomingRabbitMQMessage<Buffer> in) {
            return in.withPayload(in.getPayload().toJson());
        }
    }

}
