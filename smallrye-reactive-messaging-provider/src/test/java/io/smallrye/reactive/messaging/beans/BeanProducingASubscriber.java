package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

@ApplicationScoped
public class BeanProducingASubscriber {

  @Produces
  @Named("subscriber")
  public Subscriber<Message<String>> create() {
    return ReactiveStreams.<Message<String>>builder().ignore().build();
  }

}
