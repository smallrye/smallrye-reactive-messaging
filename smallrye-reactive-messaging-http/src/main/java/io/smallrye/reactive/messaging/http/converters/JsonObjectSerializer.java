package io.smallrye.reactive.messaging.http.converters;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.buffer.Buffer;

public class JsonObjectSerializer extends Serializer<JsonObject> {

    @Override
    public CompletionStage<Buffer> convert(JsonObject payload) {
        return CompletableFuture.completedFuture(new Buffer(payload.toBuffer()));
    }

    @Override
    public Class<? extends JsonObject> input() {
        return JsonObject.class;
    }

}
