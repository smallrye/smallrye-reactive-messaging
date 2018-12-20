package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.StreamRegistry;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@ApplicationScoped
public class InternalStreamRegistry implements StreamRegistry {

  private static final String NAME_MUST_BE_SET = "'name' must be set";
  private final Map<String, List<Publisher<? extends Message>>> publishers = new HashMap<>();
  private final Map<String, List<Subscriber<? extends Message>>> subscribers = new HashMap<>();


  @Override
  public synchronized Publisher<? extends Message> register(String name, Publisher<? extends Message> stream) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    Objects.requireNonNull(stream, "'stream' must be set");
    register(publishers, name, stream);
    return stream;
  }

  @Override
  public synchronized Subscriber<? extends Message> register(String name, Subscriber<? extends Message> subscriber) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    Objects.requireNonNull(subscriber, "'subscriber' must be set");
    register(subscribers, name, subscriber);
    return subscriber;
  }

  @Override
  public synchronized List<Publisher<? extends Message>> getPublishers(String name) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    return publishers.getOrDefault(name, Collections.emptyList());
  }

  @Override
  public synchronized List<Subscriber<? extends Message>> getSubscribers(String name) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    return subscribers.getOrDefault(name, Collections.emptyList());
  }

  private <T> void register(Map<String, List<T>> multimap, String name, T item) {
    List<T> list = multimap.computeIfAbsent(name, key -> new ArrayList<>());
    list.add(item);
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
