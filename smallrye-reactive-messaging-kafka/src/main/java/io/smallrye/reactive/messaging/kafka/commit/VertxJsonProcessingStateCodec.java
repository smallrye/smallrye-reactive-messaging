package io.smallrye.reactive.messaging.kafka.commit;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;

/**
 * Default codec for Json serialization, which use Vert.x Json support (uses Jackson underneath).
 */
public class VertxJsonProcessingStateCodec implements ProcessingStateCodec {

    public static VertxJsonProcessingStateCodec INSTANCE = new VertxJsonProcessingStateCodec();

    public static ProcessingStateCodec.Factory FACTORY = (clazz) -> INSTANCE;

    @Override
    public ProcessingState<?> decode(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return Json.decodeValue(Buffer.buffer(bytes), ProcessingState.class);
    }

    @Override
    public byte[] encode(ProcessingState<?> object) {
        return Json.encode(object).getBytes();
    }
}
