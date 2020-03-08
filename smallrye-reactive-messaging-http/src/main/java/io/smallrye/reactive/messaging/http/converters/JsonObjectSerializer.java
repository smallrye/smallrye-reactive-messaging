package io.smallrye.reactive.messaging.http.converters;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class JsonObjectSerializer extends Serializer<JsonObject> {

    @Override
    public Uni<Buffer> convert(JsonObject payload) {
        return Uni.createFrom().item(new Buffer(payload.toBuffer()));
    }

    @Override
    public Class<? extends JsonObject> input() {
        return JsonObject.class;
    }

}
