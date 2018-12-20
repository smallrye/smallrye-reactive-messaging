package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class BeanConsumingPayloadsAndReturningACompletionStageOfVoid {

  private List<String> list = new ArrayList<>();


  @Incoming("count")
  public CompletionStage<Void> consume(String payload) {
    return CompletableFuture.supplyAsync(() -> {
      list.add(payload);
      return null;
    });
  }

  public List<String> payloads() {
    return list;
  }

}
