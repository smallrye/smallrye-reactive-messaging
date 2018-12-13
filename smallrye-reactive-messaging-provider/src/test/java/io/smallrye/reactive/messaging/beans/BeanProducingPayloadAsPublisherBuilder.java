package io.smallrye.reactive.messaging.beans;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanProducingPayloadAsPublisherBuilder {


  @Outgoing("sink")
  public PublisherBuilder<String> publisher() {
    return ReactiveStreams.fromPublisher(Flowable.range(1, 10))
      .flatMapRsPublisher(i -> Flowable.just(i, i))
      .map(i -> Integer.toString(i));
  }

}
