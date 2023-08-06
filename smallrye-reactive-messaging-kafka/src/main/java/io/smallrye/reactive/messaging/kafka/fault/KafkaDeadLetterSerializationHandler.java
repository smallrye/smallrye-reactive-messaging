package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_DATA;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_DESERIALIZER;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_IS_KEY;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_KEY_DATA;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_VALUE_DATA;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.TRUE_VALUE;

import java.util.Arrays;
import java.util.Objects;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.SerializationFailureHandler;

public class KafkaDeadLetterSerializationHandler<T> implements SerializationFailureHandler<T> {

    @Override
    public byte[] decorateSerialization(Uni<byte[]> serialization, String topic, boolean isKey, String serializer, T data,
            Headers headers) {
        // deserializer failure
        if (headers.lastHeader(DESERIALIZATION_FAILURE_DESERIALIZER) != null) {
            // data exists
            Header dataHeader = headers.lastHeader(DESERIALIZATION_FAILURE_DATA);
            if (dataHeader != null) {
                // if this is the key serialization we look at the _KEY_DATA header
                if (isKey) {
                    Header isKeyHeader = headers.lastHeader(DESERIALIZATION_FAILURE_IS_KEY);
                    if (isKeyHeader != null && Arrays.equals(isKeyHeader.value(), TRUE_VALUE)) {
                        Header keyDataHeader = headers.lastHeader(DESERIALIZATION_FAILURE_KEY_DATA);
                        // fallback to data header
                        return Objects.requireNonNullElse(keyDataHeader, dataHeader).value();
                    }
                    // if this is the value serialization we look at the _VALUE_DATA header
                } else {
                    Header valueDataHeader = headers.lastHeader(DESERIALIZATION_FAILURE_VALUE_DATA);
                    // fallback to data header
                    return Objects.requireNonNullElse(valueDataHeader, dataHeader).value();
                }
            }
        }
        // call serialization
        return SerializationFailureHandler.super.decorateSerialization(serialization, topic, isKey, serializer, data, headers);
    }
}
