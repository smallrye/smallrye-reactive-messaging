package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

@ApplicationScoped
public class BeanProducingAPublisher {

  @Produces
  @Named("producer")
  public Publisher<Message<String>> create() {
    return ReactiveStreams.of("a", "b", "c").map(Message::of).buildRs();
  }

}
