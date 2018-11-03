package io.smallrye.reactive.messaging.http.converters;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.buffer.Buffer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class JsonArraySerializer extends Serializer<JsonArray> {

  public CompletionStage<Buffer> convert(JsonArray payload) {
    return CompletableFuture.completedFuture(new Buffer(payload.toBuffer()));
  }

  @Override
  public Class<? extends JsonArray> input() {
    return JsonArray.class;
  }


}
