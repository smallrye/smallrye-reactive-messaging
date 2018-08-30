package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanReturningASubscriberOfMessagesButDiscarding {

  @Incoming("subscriber")
  public Subscriber<Message<String>> create() {
    return ReactiveStreams.<Message<String>>builder()
      .ignore().build();
  }

}
