package io.smallrye.reactive.messaging.beans;

import io.smallrye.reactive.messaging.DefaultMessage;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class BeanProducingACompletableFutureOfMessage {

  @Incoming(topic = "count")
  @Outgoing(topic = "sink")
  public CompletionStage<Message<String>> process(Message<Integer> value) {
    return CompletableFuture.supplyAsync(() -> Integer.toString(value.getPayload() + 1))
      .thenApply(DefaultMessage::create);
  }
}
