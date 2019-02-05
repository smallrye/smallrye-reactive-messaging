package io.smallrye.reactive.messaging.snippets;

import io.smallrye.reactive.messaging.annotations.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class Ack {

  // tag::pre[]
  @Incoming("i")
  @Outgoing("j")
  @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
  public String autoAck(String input) {
    return input.toUpperCase();
  }
  // end::pre[]

  // tag::manual[]
  @Incoming("i")
  @Outgoing("j")
  @Acknowledgment(Acknowledgment.Strategy.MANUAL)
  public CompletionStage<Message<String>> manualAck(Message<String> input) {
    return CompletableFuture.supplyAsync(input::getPayload)
      .thenApply(Message::of)
      .thenCompose(m -> input.ack().thenApply(x -> m));
  }
  // end::manual[]

}
