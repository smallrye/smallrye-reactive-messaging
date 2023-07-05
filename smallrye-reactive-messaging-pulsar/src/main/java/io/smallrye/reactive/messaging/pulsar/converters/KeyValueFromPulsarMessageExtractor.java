package io.smallrye.reactive.messaging.pulsar.converters;

import java.lang.reflect.Type;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessageMetadata;

/**
 * Key/Value extractor extracting the key from a Pulsar message and passing the message's payload as value.
 *
 * This extractor has the default priority ({@link KeyValueExtractor#DEFAULT_PRIORITY}).
 */
@ApplicationScoped
public class KeyValueFromPulsarMessageExtractor implements KeyValueExtractor {
    @Override
    public boolean canExtract(Message<?> message, Type keyType, Type valueType) {
        Optional<PulsarIncomingMessageMetadata> metadata = message.getMetadata(PulsarIncomingMessageMetadata.class);
        // The type checks can be expensive, so, we do it only once, and rely on the fact the pulsar schema are constant.
        return metadata.filter(
                incomingMetadata -> (incomingMetadata.hasKey()
                        && TypeUtils.isAssignable(keyType, incomingMetadata.getKey().getClass())
                        && TypeUtils.isAssignable(valueType, message.getPayload().getClass())))
                .isPresent();
    }

    @Override
    public Object extractKey(Message<?> message, Type keyType) {
        return message.getMetadata(PulsarIncomingMessageMetadata.class)
                .<Object> map(PulsarIncomingMessageMetadata::getKey)
                .orElseThrow();
    }

    @Override
    public Object extractValue(Message<?> message, Type valueType) {
        return message.getPayload();
    }
}
