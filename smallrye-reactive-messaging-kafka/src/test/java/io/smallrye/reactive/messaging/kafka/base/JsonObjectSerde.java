package io.smallrye.reactive.messaging.kafka.base;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class JsonObjectSerde {

    public static class JsonObjectSerializer implements Serializer<JsonObject> {

        @Override
        public byte[] serialize(String topic, JsonObject data) {
            if (data == null) {
                return null;
            }

            return data.encode().getBytes();
        }
    }

    public static class JsonObjectDeserializer implements Deserializer<JsonObject> {
        @Override
        public JsonObject deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }

            return Buffer.buffer(data).toJsonObject();
        }
    }

}
