package io.smallrye.reactive.messaging.beans;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder {

  @Incoming(topic = "count")
  @Outgoing(topic = "sink")
  public PublisherBuilder<String> process(PublisherBuilder<Integer> source) {
    return source
      .map(i -> i + 1)
      .flatMapPublisher(i -> Flowable.just(i, i))
      .map(i -> Integer.toString(i));
  }

}
