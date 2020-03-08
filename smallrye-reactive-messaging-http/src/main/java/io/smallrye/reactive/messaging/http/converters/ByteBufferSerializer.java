package io.smallrye.reactive.messaging.http.converters;

import java.nio.ByteBuffer;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.buffer.Buffer;

public class ByteBufferSerializer extends Serializer<ByteBuffer> {

    public Uni<Buffer> convert(ByteBuffer payload) {
        return Uni.createFrom().item(new Buffer(io.vertx.core.buffer.Buffer.buffer(payload.array())));
    }

    @Override
    public Class<? extends ByteBuffer> input() {
        return ByteBuffer.class;
    }

}
