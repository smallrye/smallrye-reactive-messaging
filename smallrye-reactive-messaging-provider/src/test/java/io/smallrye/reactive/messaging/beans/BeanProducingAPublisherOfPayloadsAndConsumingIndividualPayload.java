package io.smallrye.reactive.messaging.beans;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload {

  @Incoming("count")
  @Outgoing("sink")
  public Publisher<String> process(Integer payload) {
    return ReactiveStreams.of(payload)
      .map(i -> i + 1)
      .flatMapRsPublisher(i -> Flowable.just(i, i))
      .map(i -> Integer.toString(i))
      .buildRs();
  }

}
