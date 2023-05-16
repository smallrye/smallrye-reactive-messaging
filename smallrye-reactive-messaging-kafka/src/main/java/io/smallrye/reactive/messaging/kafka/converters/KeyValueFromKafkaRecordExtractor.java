package io.smallrye.reactive.messaging.kafka.converters;

import java.lang.reflect.Type;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

/**
 * Key/Value extractor extracting the key from a Kafka record and passing the record's value as value.
 *
 * This extractor has the default priority ({@link KeyValueExtractor#DEFAULT_PRIORITY}).
 */
@ApplicationScoped
public class KeyValueFromKafkaRecordExtractor implements KeyValueExtractor {
    @Override
    public boolean canExtract(Message<?> message, Type keyType, Type valueType) {
        Optional<IncomingKafkaRecordMetadata> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
        // The type checks can be expensive, so, we do it only once, and rely on the fact the kafka deserializers are constant.
        return metadata.filter(
                incomingKafkaRecordMetadata -> TypeUtils.isAssignable(keyType, incomingKafkaRecordMetadata.getKey().getClass())
                        && TypeUtils.isAssignable(valueType, incomingKafkaRecordMetadata.getRecord().value().getClass()))
                .isPresent();
    }

    @Override
    public Object extractKey(Message<?> message, Type keyType) {
        return message.getMetadata(io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata.class)
                .map(IncomingKafkaRecordMetadata::getKey)
                .orElseThrow();
    }

    @Override
    public Object extractValue(Message<?> message, Type valueType) {
        return message.getPayload();
    }
}
