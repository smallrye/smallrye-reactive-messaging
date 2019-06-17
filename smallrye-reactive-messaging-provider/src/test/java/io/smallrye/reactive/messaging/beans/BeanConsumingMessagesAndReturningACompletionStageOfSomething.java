package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@ApplicationScoped
public class BeanConsumingMessagesAndReturningACompletionStageOfSomething {

  private List<String> list = new CopyOnWriteArrayList<>();

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  @Incoming("count")
  public CompletionStage<String> consume(Message<String> msg) {
    return CompletableFuture.supplyAsync(() -> {
      list.add(msg.getPayload());
      return "hello";
    }, executor);
  }

  public void close() {
    executor.shutdown();
  }

  public List<String> payloads() {
    return list;
  }

}
