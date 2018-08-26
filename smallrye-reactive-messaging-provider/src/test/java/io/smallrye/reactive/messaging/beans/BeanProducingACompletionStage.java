package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class BeanProducingACompletionStage {

  @Incoming("count")
  @Outgoing("sink")
  public CompletionStage<String> process(int value) {
    return CompletableFuture.supplyAsync(() -> Integer.toString(value + 1));
  }
}
