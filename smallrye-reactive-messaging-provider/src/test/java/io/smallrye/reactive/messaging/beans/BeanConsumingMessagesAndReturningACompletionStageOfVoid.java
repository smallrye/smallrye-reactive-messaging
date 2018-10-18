package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class BeanConsumingMessagesAndReturningACompletionStageOfVoid {

  private List<String> list = new ArrayList<>();


  @Incoming("count")
  public CompletionStage<Void> consume(Message<String> message) {
    return CompletableFuture.supplyAsync(() -> {
      list.add(message.getPayload());
      return null;
    });
  }

  public List<String> payloads() {
    return list;
  }

}
