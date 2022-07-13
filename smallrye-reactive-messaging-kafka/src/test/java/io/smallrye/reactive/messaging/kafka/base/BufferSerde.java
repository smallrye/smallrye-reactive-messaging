package io.smallrye.reactive.messaging.kafka.base;

import org.apache.kafka.common.serialization.Deserializer;

import io.vertx.mutiny.core.buffer.Buffer;

public class BufferSerde {

    public static class BufferDeserializer implements Deserializer<Buffer> {
        @Override
        public Buffer deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }

            return Buffer.buffer(data);
        }
    }

}
