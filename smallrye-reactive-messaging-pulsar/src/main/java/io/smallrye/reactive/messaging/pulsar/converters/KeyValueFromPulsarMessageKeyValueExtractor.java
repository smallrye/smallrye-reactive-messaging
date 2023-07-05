package io.smallrye.reactive.messaging.pulsar.converters;

import java.lang.reflect.Type;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.common.schema.KeyValue;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessageMetadata;

/**
 * Key/Value extractor extracting the key and value from a Pulsar message with KeyValue schema.
 *
 * This extractor has the default priority ({@link KeyValueExtractor#DEFAULT_PRIORITY}).
 */
@ApplicationScoped
public class KeyValueFromPulsarMessageKeyValueExtractor implements KeyValueExtractor {
    @Override
    public boolean canExtract(Message<?> message, Type keyType, Type valueType) {
        Optional<PulsarIncomingMessageMetadata> metadata = message.getMetadata(PulsarIncomingMessageMetadata.class);
        // The type checks can be expensive, so, we do it only once, and rely on the fact the pulsar schema are constant.
        return metadata.filter(
                incomingMetadata -> (message.getPayload() instanceof KeyValue
                        && TypeUtils.isAssignable(keyType, ((KeyValue) message.getPayload()).getKey().getClass())
                        && TypeUtils.isAssignable(valueType, ((KeyValue) message.getPayload()).getValue().getClass())))
                .isPresent();
    }

    @Override
    public Object extractKey(Message<?> message, Type keyType) {
        return ((KeyValue) message.getPayload()).getKey();
    }

    @Override
    public Object extractValue(Message<?> message, Type valueType) {
        return ((KeyValue) message.getPayload()).getValue();
    }
}
