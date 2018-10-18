package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class BeanConsumingPayloadsAndReturningACompletionStageOfSomething {

  private List<String> list = new ArrayList<>();


  @Incoming("count")
  public CompletionStage<String> consume(String payload) {
    return CompletableFuture.supplyAsync(() -> {
      list.add(payload);
      return "hello";
    });
  }

  public List<String> payloads() {
    return list;
  }

}
