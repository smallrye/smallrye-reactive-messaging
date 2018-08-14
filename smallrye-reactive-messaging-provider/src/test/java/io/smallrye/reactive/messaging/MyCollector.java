package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.CompletionSubscriber;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class MyCollector {

  private final List<Message<String>> result = new ArrayList<>();


  @Produces
  @Named("sink")
  public Subscriber<Message<String>> sink() {
    CompletionSubscriber<Message<String>, List<Message<String>>> subscriber = ReactiveStreams.<Message<String>>builder()
      .toList().build();
    subscriber.getCompletion().thenAccept(result::addAll);
    return subscriber;
  }

  @Produces
  @Named("count")
  public Publisher<Message<Integer>> source() {
    return Flowable.range(0, 10).map(Message::of);
  }

  public List<String> payloads() {
    return result.stream().map(Message::getPayload).collect(Collectors.toList());
  }

  public List<Message<String>> messages() {
    return result;
  }

}
