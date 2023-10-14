package connectors;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import connectors.api.ConsumedMessage;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

@ApplicationScoped
public class MyMessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.isAssignable(target, ConsumedMessage.class)
                && in.getMetadata(MyIncomingMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(in.getMetadata(MyIncomingMetadata.class)
                .map(MyIncomingMetadata::getCustomMessage)
                .orElse(null));
    }
}
