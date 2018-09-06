package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class BeanReturningCompletionStageOfPayload {

  private AtomicInteger count = new AtomicInteger();

  @Outgoing("infinite-producer")
  public CompletionStage<Integer> create() {
    return CompletableFuture.supplyAsync(() -> count.incrementAndGet());
  }

}
