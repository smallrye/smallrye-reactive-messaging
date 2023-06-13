package io.smallrye.reactive.messaging.pulsar.converters;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Messages;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessageMetadata;

@ApplicationScoped
public class PulsarBatchMessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getMetadata(PulsarIncomingBatchMessageMetadata.class).isPresent()
                && TypeUtils.getRawTypeIfParameterized(target).equals(Messages.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        PulsarIncomingBatchMessageMetadata metadata = in.getMetadata(PulsarIncomingBatchMessageMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No Pulsar metadata"));
        // The consumer record is not directly accessible:
        return in.withPayload(metadata.getMessages());
    }
}
