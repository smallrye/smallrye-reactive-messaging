package io.smallrye.reactive.messaging.kafka;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageKeyValueExtractor;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
public class ConsumerRecordKeyValueExtractor implements MessageKeyValueExtractor {

    @Override
    public boolean canExtract(Message<?> in, Type keyType, Type valueType) {
        return in.getMetadata(IncomingKafkaRecordMetadata.class).isPresent();
    }

    @Override
    public Object extractKey(Message<?> in, Type target) {
        IncomingKafkaRecordMetadata metadata = in.getMetadata(IncomingKafkaRecordMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No Kafka metadata"));
        return metadata.getKey();
    }

    @Override
    public Object extractValue(Message<?> in, Type target) {
        IncomingKafkaRecordMetadata metadata = in.getMetadata(IncomingKafkaRecordMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No Kafka metadata"));
        return in.getPayload();
    }
}
