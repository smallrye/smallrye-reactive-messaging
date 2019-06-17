package io.smallrye.reactive.messaging.http.converters;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.vertx.reactivex.core.buffer.Buffer;

public class ByteBufferSerializer extends Serializer<ByteBuffer> {

    public CompletionStage<Buffer> convert(ByteBuffer payload) {
        return CompletableFuture.completedFuture(new Buffer(io.vertx.core.buffer.Buffer.buffer(payload.array())));
    }

    @Override
    public Class<? extends ByteBuffer> input() {
        return ByteBuffer.class;
    }

}
