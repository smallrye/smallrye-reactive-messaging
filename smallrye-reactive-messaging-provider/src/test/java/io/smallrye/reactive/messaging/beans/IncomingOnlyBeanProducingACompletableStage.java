package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class IncomingOnlyBeanProducingACompletableStage {

  private List<Integer> list = new ArrayList<>();

  @Incoming("count")
  public CompletionStage<Void> process(Message<String> value) {
    System.out.println("Getting some value " + value.getPayload());
    list.add(Integer.valueOf(value.getPayload()));
    return CompletableFuture.completedFuture(null);
  }

  public List<Integer> list() {
    return list;
  }

}
