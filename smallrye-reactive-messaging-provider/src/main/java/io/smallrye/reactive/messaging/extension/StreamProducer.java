package io.smallrye.reactive.messaging.extension;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;

import io.smallrye.reactive.messaging.annotations.Emitter;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.CompletionSubscriber;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.annotations.Stream;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@ApplicationScoped
public class StreamProducer {

    @Inject
    StreamRegistry streamRegistry;

    @Produces
    @Stream("") // Stream name is ignored during type-safe resolution
    <T> Flowable<T> producePublisher(InjectionPoint injectionPoint) {
        Type first = getFirstParameter(injectionPoint.getType());
        if (TypeUtils.isAssignable(first, Message.class)) {
            return cast(Flowable.fromPublisher(getPublisher(injectionPoint)));
        } else {
            return cast(Flowable.fromPublisher(getPublisher(injectionPoint))
                    .map(Message::getPayload));
        }
    }

    @Produces
    @Stream("") // Stream name is ignored during type-safe resolution
    <T> PublisherBuilder<T> producePublisherBuilder(InjectionPoint injectionPoint) {
        Type first = getFirstParameter(injectionPoint.getType());
        if (TypeUtils.isAssignable(first, Message.class)) {
            return cast(ReactiveStreams.fromPublisher(getPublisher(injectionPoint)));
        } else {
            return cast(ReactiveStreams.fromPublisher(getPublisher(injectionPoint))
                    .map(Message::getPayload));
        }
    }

  @Produces
  @Stream("") // Stream name is ignored during type-safe resolution
  <T> Emitter<T> produceEmitter(InjectionPoint injectionPoint) {
    SubscriberBuilder<? extends Message, Void> subscriber = getSubscriberBuilder(injectionPoint);
    @SuppressWarnings("unchecked") EmitterImpl emitter = new EmitterImpl(subscriber.build());
    return cast(emitter);
  }

    @SuppressWarnings("rawtypes")
    private Publisher<? extends Message> getPublisher(InjectionPoint injectionPoint) {
        String name = getStreamName(injectionPoint);
        List<PublisherBuilder<? extends Message>> list = streamRegistry.getPublishers(name);
        if (list.isEmpty()) {
            throw new IllegalStateException("Unable to find a stream with the name " + name + ", available streams are: " + streamRegistry.getIncomingNames());
        }
        // TODO Manage merge.
        return list.get(0).buildRs();
    }

  @SuppressWarnings("rawtypes")
  private SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(InjectionPoint injectionPoint) {
    String name = getStreamName(injectionPoint);
    List<SubscriberBuilder<? extends Message, Void>> list = streamRegistry.getSubscribers(name);
    if (list.isEmpty()) {
      throw new IllegalStateException("Unable to find a stream with the name " + name + ", available streams are: " + streamRegistry.getOutgoingNames());
    }
    return list.get(0);
  }

    private Type getFirstParameter(Type type) {
        if (type instanceof ParameterizedType) {
            return ((ParameterizedType) type).getActualTypeArguments()[0];
        }
        return null;
    }

    static String getStreamName(InjectionPoint injectionPoint) {
        Stream qualifier = getStreamQualifier(injectionPoint);
        if (qualifier == null) {
            throw new IllegalStateException("@Stream qualifier not found on + " + injectionPoint);
        }
        return qualifier.value();
    }

    static Stream getStreamQualifier(InjectionPoint injectionPoint) {
        for (Annotation qualifier : injectionPoint.getQualifiers()) {
            if (qualifier.annotationType()
                    .equals(Stream.class)) {
                return (Stream) qualifier;
            }
        }
        return null;
    }


    @SuppressWarnings("unchecked")
    private static <T> T cast(Object obj) {
        return (T) obj;
    }

}
