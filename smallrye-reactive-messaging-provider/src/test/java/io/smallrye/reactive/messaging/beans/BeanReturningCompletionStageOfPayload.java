package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class BeanReturningCompletionStageOfPayload {

  private AtomicInteger count = new AtomicInteger();
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  public void close() {
    executor.shutdown();
  }

  @Outgoing("infinite-producer")
  public CompletionStage<Integer> create() {
    return CompletableFuture.supplyAsync(() -> count.incrementAndGet(), executor);
  }

  @PreDestroy
  public void cleanup() {
    executor.shutdown();
  }

}
