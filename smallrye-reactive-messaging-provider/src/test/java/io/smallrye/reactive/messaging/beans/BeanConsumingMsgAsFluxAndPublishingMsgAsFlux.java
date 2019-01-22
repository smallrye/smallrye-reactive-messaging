package io.smallrye.reactive.messaging.beans;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import reactor.core.publisher.Flux;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanConsumingMsgAsFluxAndPublishingMsgAsFlux {

  @Incoming("count")
  @Outgoing("sink")
  public Flux<Message<String>> process(Flux<Message<Integer>> source) {
    return source
      .map(Message::getPayload)
      .map(i -> i + 1)
      .flatMap(i -> Flowable.just(i, i))
      .map(i -> Integer.toString(i))
      .map(Message::of);
  }

}
