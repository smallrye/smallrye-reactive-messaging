package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.SubscriberBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

@ApplicationScoped
public class BeanProducingASubscriberBuilder {

  @Produces
  @Named("subscriber")
  public SubscriberBuilder<Message<String>, Void> create() {
    return ReactiveStreams.<Message<String>>builder().ignore();
  }

}
