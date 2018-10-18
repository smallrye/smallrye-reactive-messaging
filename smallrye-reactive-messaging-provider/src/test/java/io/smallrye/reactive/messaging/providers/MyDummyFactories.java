package io.smallrye.reactive.messaging.providers;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class MyDummyFactories implements PublisherFactory, SubscriberFactory {
  private final List<String> list = new ArrayList<>();
  private boolean completed = false;

  @Override
  public Class<? extends MessagingProvider> type() {
    return Dummy.class;
  }

  public void reset() {
    list.clear();
    completed = false;
  }

  public List<String> list() {
    return list;
  }

  @Override
  public CompletionStage<Subscriber<? extends Message>> createSubscriber(Map<String, String> config) {
    return CompletableFuture.completedFuture(ReactiveStreams.<Message>builder()
      .peek(x -> list.add(x.getPayload().toString()))
      .onComplete(() -> completed = true)
      .ignore()
      .build()
    );
  }

  @Override
  public CompletionStage<Publisher<? extends Message>> createPublisher(Map<String, String> config) {
    int increment = Integer.parseInt(config.getOrDefault("increment", "1"));
    return CompletableFuture.completedFuture(Flowable.just(1, 2, 3).map(i -> i + increment).map(Message::of));
  }

  public boolean gotCompletion() {
    return completed;
  }
}
