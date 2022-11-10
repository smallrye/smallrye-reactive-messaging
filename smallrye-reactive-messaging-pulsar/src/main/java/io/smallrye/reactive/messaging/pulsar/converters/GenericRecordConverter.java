package io.smallrye.reactive.messaging.pulsar.converters;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessageMetadata;

@ApplicationScoped
public class GenericRecordConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getMetadata(PulsarIncomingMessageMetadata.class).isPresent()
                && TypeUtils.isAssignable(target, GenericRecord.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        PulsarIncomingMessageMetadata metadata = in.getMetadata(PulsarIncomingMessageMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No Pulsar metadata"));
        // The consumer record is not directly accessible:
        org.apache.pulsar.client.api.Message<?> message = metadata.getMessage();
        return in.withPayload(message.getValue());
    }
}
