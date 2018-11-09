package io.smallrye.reactive.messaging.eventbus;

import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class VertxEventBusMessagingProvider implements SubscriberFactory, PublisherFactory {

  @Inject
  private Vertx vertx;


  @Override
  public CompletionStage<Publisher<? extends Message>> createPublisher(Map<String, String> config) {
    return CompletableFuture.completedFuture(new EventBusSource(vertx, ConfigurationHelper.create(config)).publisher());
  }

  @Override
  public Class<? extends MessagingProvider> type() {
    return VertxEventBus.class;
  }

  @Override
  public CompletionStage<Subscriber<? extends Message>> createSubscriber(Map<String, String> config) {
    return CompletableFuture.completedFuture(new EventBusSink(vertx, ConfigurationHelper.create(config)).subscriber());
  }
}
