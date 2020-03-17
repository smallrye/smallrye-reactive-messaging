package io.smallrye.reactive.messaging.http.converters;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.buffer.Buffer;

public class StringSerializer extends Serializer<String> {
    @Override
    public Uni<Buffer> convert(String payload) {
        return Uni.createFrom().item(Buffer.buffer().appendString(payload));
    }

    @Override
    public Class<? extends String> input() {
        return String.class;
    }
}
