package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class BeanConsumingMessagesAndReturningACompletionStageOfSomething {

  private List<String> list = new CopyOnWriteArrayList<>();


  @Incoming("count")
  public CompletionStage<String> consume(Message<String> msg) {
    return CompletableFuture.supplyAsync(() -> {
      list.add(msg.getPayload());
      return "hello";
    });
  }

  public List<String> payloads() {
    return list;
  }

}
