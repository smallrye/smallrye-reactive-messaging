package io.smallrye.reactive.messaging.beans;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanConsumingItemAsFlowableAndPublishingItemAsFlowable {

  @Incoming("count")
  @Outgoing("sink")
  public Flowable<String> process(Flowable<Integer> source) {
    return source
      .map(i -> i + 1)
      .flatMap(i -> Flowable.just(i, i))
      .map(i -> Integer.toString(i));
  }

}
