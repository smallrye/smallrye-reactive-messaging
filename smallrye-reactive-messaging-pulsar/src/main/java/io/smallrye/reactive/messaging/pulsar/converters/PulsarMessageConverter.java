package io.smallrye.reactive.messaging.pulsar.converters;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessageMetadata;

@ApplicationScoped
public class PulsarMessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getMetadata(PulsarIncomingMessageMetadata.class).isPresent()
                && target.equals(org.apache.pulsar.client.api.Message.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        PulsarIncomingMessageMetadata metadata = in.getMetadata(PulsarIncomingMessageMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No Pulsar metadata"));
        // The consumer record is not directly accessible:
        return in.withPayload(metadata.getMessage());
    }
}
