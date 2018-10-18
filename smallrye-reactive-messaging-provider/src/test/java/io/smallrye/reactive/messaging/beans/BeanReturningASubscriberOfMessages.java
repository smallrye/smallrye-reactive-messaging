package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class BeanReturningASubscriberOfMessages {

  private List<String> list = new ArrayList<>();


  @Incoming("count")
  public Subscriber<Message<String>> create() {
    return ReactiveStreams.<Message<String>>builder().forEach(m -> list.add(m.getPayload()))
      .build();
  }

  public List<String> payloads() {
    return list;
  }

}
