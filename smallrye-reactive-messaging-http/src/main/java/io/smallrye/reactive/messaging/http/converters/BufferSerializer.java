package io.smallrye.reactive.messaging.http.converters;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.vertx.reactivex.core.buffer.Buffer;

public class BufferSerializer extends Serializer<io.vertx.core.buffer.Buffer> {

    @Override
    public CompletionStage<Buffer> convert(io.vertx.core.buffer.Buffer payload) {
        return CompletableFuture.completedFuture(new Buffer(payload));
    }

    @Override
    public Class<? extends io.vertx.core.buffer.Buffer> input() {
        return io.vertx.core.buffer.Buffer.class;
    }

}
