package io.smallrye.reactive.messaging.http.converters;

import io.vertx.reactivex.core.buffer.Buffer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class StringSerializer extends Serializer<String> {
  @Override
  public CompletionStage<Buffer> convert(String payload) {
    return CompletableFuture.completedFuture(Buffer.buffer().appendString(payload));
  }

  @Override
  public Class<? extends String> input() {
    return String.class;
  }
}
