package io.smallrye.reactive.messaging.http.converters;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.buffer.Buffer;

public class BufferSerializer extends Serializer<io.vertx.core.buffer.Buffer> {

    @Override
    public Uni<Buffer> convert(io.vertx.core.buffer.Buffer payload) {
        return Uni.createFrom().item(new Buffer(payload));
    }

    @Override
    public Class<? extends io.vertx.core.buffer.Buffer> input() {
        return io.vertx.core.buffer.Buffer.class;
    }

}
