package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@ApplicationScoped
public class BeanConsumingPayloadsAndReturningACompletionStageOfSomething {

  private List<String> list = new CopyOnWriteArrayList<>();

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  public void close() {
    executor.shutdown();
  }

  @Incoming("count")
  public CompletionStage<String> consume(String payload) {
    return CompletableFuture.supplyAsync(() -> {
      list.add(payload);
      return "hello";
    }, executor);
  }

  public List<String> payloads() {
    return list;
  }

}
