package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanReturningAPublisherBuilderOfItems {

  @Outgoing("producer")
  public Publisher<String> create() {
    return ReactiveStreams.of("a", "b", "c").buildRs();
  }

}
