package io.smallrye.reactive.messaging.http.converters;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.mutiny.core.buffer.Buffer;

public class JsonArraySerializer extends Serializer<JsonArray> {

    public Uni<Buffer> convert(JsonArray payload) {
        return Uni.createFrom().item(new Buffer(payload.toBuffer()));
    }

    @Override
    public Class<? extends JsonArray> input() {
        return JsonArray.class;
    }

}
