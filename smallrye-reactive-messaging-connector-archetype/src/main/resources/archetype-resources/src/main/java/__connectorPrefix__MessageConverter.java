package ${package};

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import ${package}.api.ConsumedMessage;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

@ApplicationScoped
public class ${connectorPrefix}MessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.isAssignable(target, ConsumedMessage.class)
                && in.getMetadata(${connectorPrefix}IncomingMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(in.getMetadata(${connectorPrefix}IncomingMetadata.class)
                .map(${connectorPrefix}IncomingMetadata::getCustomMessage)
                .orElse(null));
    }
}
