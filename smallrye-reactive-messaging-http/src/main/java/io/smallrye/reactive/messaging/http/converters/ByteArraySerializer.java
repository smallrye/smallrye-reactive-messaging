package io.smallrye.reactive.messaging.http.converters;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.vertx.reactivex.core.buffer.Buffer;

public class ByteArraySerializer extends Serializer<byte[]> {

    public CompletionStage<Buffer> convert(byte[] payload) {
        return CompletableFuture.completedFuture(new Buffer(io.vertx.core.buffer.Buffer.buffer(payload)));
    }

    @Override
    public Class<? extends byte[]> input() {
        return byte[].class;
    }

}
