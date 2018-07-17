package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Optional;
import java.util.Set;

public interface StreamRegistry {


  Publisher<? extends Message> register(String name, Publisher<? extends Message> stream);

  Subscriber<? extends Message> register(String name, Subscriber<? extends Message> subscriber);

  Optional<Flowable<? extends Message>> getPublisher(String name);

  Optional<Subscriber<? extends Message>> getSubscriber(String name);

  Publisher<? extends Message> unregisterPublisher(String name);

  Subscriber<? extends Message> unregisterSubscriber(String name);

  Set<String> getPublisherNames();

  Set<String> getSubscriberNames();

}
