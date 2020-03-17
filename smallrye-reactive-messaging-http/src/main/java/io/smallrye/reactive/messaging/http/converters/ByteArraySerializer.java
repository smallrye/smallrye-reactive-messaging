package io.smallrye.reactive.messaging.http.converters;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.buffer.Buffer;

public class ByteArraySerializer extends Serializer<byte[]> {

    public Uni<Buffer> convert(byte[] payload) {
        return Uni.createFrom().item(new Buffer(io.vertx.core.buffer.Buffer.buffer(payload)));
    }

    @Override
    public Class<? extends byte[]> input() {
        return byte[].class;
    }

}
