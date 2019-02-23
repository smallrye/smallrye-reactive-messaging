package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.StreamRegistry;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import javax.enterprise.context.ApplicationScoped;
import java.util.*;

@ApplicationScoped
public class InternalStreamRegistry implements StreamRegistry {

  private static final String NAME_MUST_BE_SET = "'name' must be set";
  private final Map<String, List<PublisherBuilder<? extends Message>>> publishers = new HashMap<>();
  private final Map<String, List<SubscriberBuilder<? extends Message, Void>>> subscribers = new HashMap<>();


  @Override
  public synchronized PublisherBuilder<? extends Message> register(String name, PublisherBuilder<? extends Message> stream) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    Objects.requireNonNull(stream, "'stream' must be set");
    register(publishers, name, stream);
    return stream;
  }

  @Override
  public synchronized SubscriberBuilder<? extends Message, Void> register(String name, SubscriberBuilder<? extends Message, Void> subscriber) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    Objects.requireNonNull(subscriber, "'subscriber' must be set");
    register(subscribers, name, subscriber);
    return subscriber;
  }

  @Override
  public synchronized List<PublisherBuilder<? extends Message>> getPublishers(String name) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    return publishers.getOrDefault(name, Collections.emptyList());
  }

  @Override
  public synchronized List<SubscriberBuilder<? extends Message, Void>> getSubscribers(String name) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    return subscribers.getOrDefault(name, Collections.emptyList());
  }

  private <T> void register(Map<String, List<T>> multimap, String name, T item) {
    List<T> list = multimap.computeIfAbsent(name, key -> new ArrayList<>());
    list.add(item);
  }

  @Override
  public synchronized Set<String> getIncomingNames() {
    return new HashSet<>(publishers.keySet());
  }

  @Override
  public synchronized Set<String> getOutgoingNames() {
    return new HashSet<>(subscribers.keySet());
  }

}
