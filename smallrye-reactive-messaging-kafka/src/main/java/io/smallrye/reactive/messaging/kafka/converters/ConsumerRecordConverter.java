package io.smallrye.reactive.messaging.kafka.converters;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

/**
 * Convert an incoming Kafka message into a {@link ConsumerRecord}.
 */
@ApplicationScoped
public class ConsumerRecordConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getMetadata(IncomingKafkaRecordMetadata.class).isPresent()
                && target.equals(ConsumerRecord.class);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingKafkaRecordMetadata metadata = in.getMetadata(IncomingKafkaRecordMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No Kafka metadata"));
        // The consumer record is not directly accessible:
        return in.withPayload(metadata.getRecord());
    }
}
