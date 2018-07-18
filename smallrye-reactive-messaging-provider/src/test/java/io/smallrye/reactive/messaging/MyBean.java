package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class MyBean {

  static final List<String> COLLECTOR = new ArrayList<>();

  @Incoming(topic = "my-dummy-stream")
  @Outgoing(topic = "toUpperCase")
  public Publisher<String> toUppercase(Flowable<String> input) {
    return input.map(String::toUpperCase);
  }

  @Incoming(topic = "toUpperCase")
  @Outgoing(topic = "my-output")
  public PublisherBuilder<String> duplicate(PublisherBuilder<String> input) {
    return input.flatMap(s -> ReactiveStreams.of(s, s));
  }


  @Produces
  @Named("my-dummy-stream")
  Publisher<Message<String>> stream() {
    return Flowable.just("foo", "bar").map(DefaultMessage::create);
  }

  @Produces
  @Named("my-output")
  Subscriber<Message<String>> output() {
    return ReactiveStreams.<Message<String>>builder().forEach(s -> COLLECTOR.add(s.getPayload())).build();
  }
}
