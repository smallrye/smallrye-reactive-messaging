package io.smallrye.reactive.messaging.kafka.converters;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;

/**
 * Convert an incoming Kafka batch message into a {@link ConsumerRecords}.
 */
@ApplicationScoped
public class ConsumerRecordsConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getMetadata(IncomingKafkaRecordBatchMetadata.class).isPresent()
                && target.equals(ConsumerRecords.class);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingKafkaRecordBatchMetadata metadata = in.getMetadata(IncomingKafkaRecordBatchMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No Kafka metadata"));
        return in.withPayload(metadata.getRecords());
    }
}
