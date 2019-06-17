package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@ApplicationScoped
public class BeanConsumingMessagesAndReturningACompletionStageOfVoid {

  private List<String> list = new CopyOnWriteArrayList<>();

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  public void close() {
    executor.shutdown();
  }

  @Incoming("count")
  public CompletionStage<Void> consume(Message<String> message) {
    return CompletableFuture.supplyAsync(() -> {
      list.add(message.getPayload());
      return null;
    }, executor);
  }

  public List<String> payloads() {
    return list;
  }

}
