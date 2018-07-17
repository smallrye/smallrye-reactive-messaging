package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class DefaultMessage<T> implements Message<T> {

  private final T payload;

  public static <T> Message<T> create(T payload) {
    return new DefaultMessage<>(payload);
  }

  private DefaultMessage(T payload) {
    this.payload = payload;
  }

  @Override
  public T getPayload() {
    return payload;
  }

  @Override
  public CompletionStage<Void> ack() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return "DefaultMessage{" +
      "payload=" + payload +
      '}';
  }
}
