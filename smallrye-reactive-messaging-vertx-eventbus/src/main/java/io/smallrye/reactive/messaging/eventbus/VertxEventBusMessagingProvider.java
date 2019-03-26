package io.smallrye.reactive.messaging.eventbus;

import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.smallrye.reactive.messaging.spi.IncomingConnectorFactory;
import io.smallrye.reactive.messaging.spi.OutgoingConnectorFactory;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class VertxEventBusMessagingProvider implements OutgoingConnectorFactory, IncomingConnectorFactory {

  @Inject
  private Vertx vertx;


  @Override
  public PublisherBuilder<? extends Message> getPublisherBuilder(Config config) {
    return new EventBusSource(vertx, config).source();
  }

  @Override
  public SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config) {
    return new EventBusSink(vertx, config).sink();
  }

  @Override
  public Class<? extends MessagingProvider> type() {
    return VertxEventBus.class;
  }

}
