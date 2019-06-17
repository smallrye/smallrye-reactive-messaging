package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@ApplicationScoped
public class BeanConsumingPayloadsAndReturningACompletionStageOfVoid {

  private List<String> list = new CopyOnWriteArrayList<>();

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  @Incoming("count")
  public CompletionStage<Void> consume(String payload) {
    return CompletableFuture.supplyAsync(() -> {
      list.add(payload);
      return null;
    }, executor);
  }

  public void close() {
    executor.shutdown();
  }

  public List<String> payloads() {
    return list;
  }

}
