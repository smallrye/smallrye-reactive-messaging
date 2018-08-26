package io.smallrye.reactive.messaging.impl;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.StreamRegistry;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.*;

@ApplicationScoped
public class StreamRegistryImpl implements StreamRegistry {

  private static final String NAME_MUST_BE_SET = "'name' must be set";
  private final Map<String, Publisher<? extends Message>> publishers = new HashMap<>();
  private final Map<String, Subscriber<? extends Message>> subscribers = new HashMap<>();


  @Override
  public synchronized Publisher<? extends Message> register(String name, Publisher<? extends Message> stream) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    Objects.requireNonNull(stream, "'stream' must be set");
    publishers.put(name, Flowable.fromPublisher(stream));
    return stream;
  }

  @Override
  public synchronized Subscriber<? extends Message> register(String name, Subscriber<? extends Message> subscriber) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    Objects.requireNonNull(subscriber, "'subscriber' must be set");
    subscribers.put(name, subscriber);
    return subscriber;
  }

  @Override
  public synchronized Optional<Publisher<? extends Message>> getPublisher(String name) {
    // TODO Should we also check for the config provider?
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    System.out.println("Retrieving publisher: " + name + " / " + publishers.get(name));
    return Optional.ofNullable(publishers.get(name));
  }

  @Override
  public synchronized Optional<Subscriber<? extends Message>> getSubscriber(String name) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    return Optional.ofNullable(subscribers.get(name));
  }

  @Override
  public synchronized Publisher<? extends Message> unregisterPublisher(String name) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    return publishers.remove(name);
  }

  @Override
  public synchronized Subscriber<? extends Message> unregisterSubscriber(String name) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    return subscribers.remove(name);
  }

  @Override
  public synchronized Set<String> getPublisherNames() {
    return new HashSet<>(publishers.keySet());
  }

  @Override
  public synchronized Set<String> getSubscriberNames() {
    return new HashSet<>(subscribers.keySet());
  }

}
