package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

@ApplicationScoped
public class MyBean {

  @Incoming(topic = "my-stream")
  @Named("toUpperCase")
  public Publisher<String> toUppercase(Flowable<String> input) {
    return input.map(String::toUpperCase);
  }

  @Incoming(topic = "toUpperCase")
  @Outgoing(topic = "my-output")
  public PublisherBuilder<String> duplicate(PublisherBuilder<String> input) {
    return input.flatMap(s -> ReactiveStreams.of(s, s));
  }


  @Produces
  @Named("my-stream")
  Publisher<String> stream() {
    return Flowable.just("foo", "bar");
  }

  @Produces
  @Named("my-output")
  // TODO Also support producing SubscriberBuilder, ProcessorBuilder, PublisherBuilder
  Subscriber<String> output() {
    return ReactiveStreams.<String>builder().forEach(s -> System.out.println("received " + s)).build();
  }
}
