package io.smallrye.reactive.messaging.providers;

import io.reactivex.Flowable;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
@Connector("dummy")
public class MyDummyFactories implements IncomingConnectorFactory, OutgoingConnectorFactory {
  private final List<String> list = new ArrayList<>();
  private boolean completed = false;

  public void reset() {
    list.clear();
    completed = false;
  }

  public List<String> list() {
    return list;
  }

  @Override
  public SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config) {
    return ReactiveStreams.<Message>builder()
      .peek(x -> list.add(x.getPayload().toString()))
      .onComplete(() -> completed = true)
      .ignore();
  }

  @Override
  public PublisherBuilder<? extends Message> getPublisherBuilder(Config config) {
    int increment = config.getOptionalValue("increment", Integer.class).orElse(1);
    return ReactiveStreams
      .fromPublisher(Flowable.just(1, 2, 3).map(i -> i + increment).map(Message::of));
  }

  public boolean gotCompletion() {
    return completed;
  }
}
