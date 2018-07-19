package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.StreamFactory;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class StreamFactoryImpl implements StreamFactory {

  private static final String NAME_MUST_BE_SET = "'name' must be set";

  private final Vertx vertx;
  private final StreamRegistry registry;

  private final Map<String, PublisherFactory> publisherFactories = new HashMap<>();
  private Map<String, SubscriberFactory> subscriberFactories = new HashMap<>();


  @Inject
  public StreamFactoryImpl(@Any Instance<PublisherFactory> pubs,
                           @Any Instance<SubscriberFactory> subs,
                           StreamRegistry registry,
                           ReactiveMessagingExtension extension) {
    this.vertx = extension.vertx();
    this.registry = registry;
    pubs.stream().forEach(pf -> publisherFactories.put(pf.type().getName(), pf));
    subs.stream().forEach(pf -> subscriberFactories.put(pf.type().getName(), pf));
  }

  @Override
  public synchronized CompletionStage<Publisher<? extends Message>> createPublisherAndRegister(String name, Map<String, String> config) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    String type = Optional.ofNullable(config.get("type")).map(Object::toString)
      .orElseThrow(() -> new IllegalArgumentException("Invalid publisher, no type for " + name));
    return createPublisher(type, config)
      .thenApply(publisher -> this.registry.register(name, publisher));
  }

  @Override
  public synchronized CompletionStage<Subscriber<? extends Message>> createSubscriberAndRegister(String name, Map<String, String> config) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    String type = Optional.ofNullable(config.get("type")).map(Object::toString)
      .orElseThrow(() -> new IllegalArgumentException("Invalid subscriber, no type for " + name));
    return createSubscriber(type, config)
      .thenApply(subscriber -> this.registry.register(name, subscriber));
  }

  @Override
  public synchronized CompletionStage<Publisher<? extends Message>> createPublisher(String type, Map<String, String> config) {
    PublisherFactory factory = publisherFactories.get(
      Objects.requireNonNull(type, "'type' must be set, known types are: " + publisherFactories.keySet()));
    if (factory == null) {
      throw new IllegalArgumentException("Unknown type: " + type + ", known types are: " + publisherFactories.keySet());
    }
    // TODO Can we have null configuration ?
    return factory.createPublisher(vertx, config);
  }

  @Override
  public CompletionStage<Subscriber<? extends Message>> createSubscriber(String type, Map<String, String> config) {
    SubscriberFactory factory = subscriberFactories.get(
      Objects.requireNonNull(type, "'type' must be set, known types are: " + subscriberFactories.keySet()));
    if (factory == null) {
      throw new IllegalArgumentException("Unknown type: " + type + ", known types are: " + subscriberFactories.keySet());
    }
    // TODO Can we have null configuration ?
    return factory.createSubscriber(vertx, config);
  }
}
