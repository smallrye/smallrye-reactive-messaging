package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class IncomingOnlyBeanProducingANonVoidCompletableStage {

  private List<Integer> list = new ArrayList<>();

  @Incoming("count")
  public CompletionStage<String> process(Message<String> value) {
    list.add(Integer.valueOf(value.getPayload()));
    return CompletableFuture.completedFuture("hello");
  }

  public List<Integer> list() {
    return list;
  }

}
