package io.smallrye.reactive.messaging.http;

import io.smallrye.reactive.messaging.http.converters.Serializer;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.buffer.Buffer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class PersonSerializer extends Serializer<Person> {
  @Override
  public CompletionStage<Buffer> convert(Person payload) {
    return CompletableFuture.completedFuture(Buffer.buffer().appendString(Json.encode(payload)));
  }

  @Override
  public Class<? extends Person> input() {
    return Person.class;
  }
}
