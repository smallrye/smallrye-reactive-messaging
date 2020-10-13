package io.smallrye.reactive.messaging.kafka.tracing;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

/**
 * Interface to deal with OpenTelemetry calls. Not all environments have OpenTelemetry available.
 *
 */
public interface OpenTelemetryTracer {

    void createOutgoingTrace(Message<?> message, String topic, int partition, Headers headers);

    <K, V> void createIncomingTrace(IncomingKafkaRecord<K, V> kafkaRecord);

    <K, T> TracingMetadata extractTracingMetadata(IncomingKafkaRecordMetadata<K, T> kafkaMetadata,
            KafkaConsumerRecord<K, T> record);
}
