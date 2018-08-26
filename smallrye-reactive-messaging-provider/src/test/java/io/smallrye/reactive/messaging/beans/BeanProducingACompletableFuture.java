package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
public class BeanProducingACompletableFuture {

  @Incoming("count")
  @Outgoing("sink")
  public CompletableFuture<String> process(int value) {
    return CompletableFuture.supplyAsync(() -> Integer.toString(value + 1));
  }
}
