package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.Set;

public interface StreamRegistry {


  Publisher<? extends Message> register(String name, Publisher<? extends Message> stream);

  Subscriber<? extends Message> register(String name, Subscriber<? extends Message> subscriber);

  List<Publisher<? extends Message>> getPublishers(String name);

  List<Subscriber<? extends Message>> getSubscribers(String name);

  Set<String> getPublisherNames();

  Set<String> getSubscriberNames();

}
