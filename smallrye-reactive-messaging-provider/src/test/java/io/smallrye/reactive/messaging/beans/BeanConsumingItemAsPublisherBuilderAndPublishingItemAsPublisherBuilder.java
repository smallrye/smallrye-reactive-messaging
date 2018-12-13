package io.smallrye.reactive.messaging.beans;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder {

  @Incoming("count")
  @Outgoing("sink")
  public PublisherBuilder<String> process(PublisherBuilder<Integer> source) {
    return source
      .map(i -> i + 1)
      .flatMapRsPublisher(i -> Flowable.just(i, i))
      .map(i -> Integer.toString(i));
  }

}
