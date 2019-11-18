package io.smallrye.reactive.messaging.extension;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;

import org.apache.commons.lang3.reflect.TypeUtils;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.Emitter;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Stream;

@ApplicationScoped
public class ChannelProducer {

    @Inject
    ChannelRegistry channelRegistry;

    @Produces
    @Channel("") // Stream name is ignored during type-safe resolution
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
    @Stream("")
    <T> Flowable<T> producePublisherLegacy(InjectionPoint injectionPoint) {
        return producePublisher(injectionPoint);
    }

    @Produces
    @Channel("") // Stream name is ignored during type-safe resolution
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
    @Stream("")
    <T> PublisherBuilder<T> producePublisherBuilderLegacy(InjectionPoint injectionPoint) {
        return producePublisherBuilder(injectionPoint);
    }

    @Produces
    @Channel("") // Stream name is ignored during type-safe resolution
    <T> Emitter<T> produceEmitter(InjectionPoint injectionPoint) {
        Emitter emitter = getEmitter(injectionPoint);
        return cast(emitter);
    }

    @Produces
    @Stream("")
    <T> Emitter<T> produceEmitterLegacy(InjectionPoint injectionPoint) {
        return produceEmitter(injectionPoint);
    }

    @SuppressWarnings("rawtypes")
    private Publisher<? extends Message> getPublisher(InjectionPoint injectionPoint) {
        String name = getChannelName(injectionPoint);
        List<PublisherBuilder<? extends Message>> list = channelRegistry.getPublishers(name);
        if (list.isEmpty()) {
            throw new IllegalStateException("Unable to find a stream with the name " + name + ", available streams are: "
                    + channelRegistry.getIncomingNames());
        }
        // TODO Manage merge.
        return list.get(0).buildRs();
    }

    @SuppressWarnings("rawtypes")
    private SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(InjectionPoint injectionPoint) {
        String name = getChannelName(injectionPoint);
        List<SubscriberBuilder<? extends Message, Void>> list = channelRegistry.getSubscribers(name);
        if (list.isEmpty()) {
            throw new IllegalStateException("Unable to find a stream with the name " + name + ", available streams are: "
                    + channelRegistry.getOutgoingNames());
        }
        return list.get(0);
    }

    @SuppressWarnings("rawtypes")
    private Emitter getEmitter(InjectionPoint injectionPoint) {
        String name = getChannelName(injectionPoint);
        Emitter emitter = channelRegistry.getEmitter(name);
        if (emitter == null) {
            throw new IllegalStateException("Unable to find a emitter with the name " + name + ", available emitters are: "
                    + channelRegistry.getEmitterNames());
        }
        return emitter;
    }

    private Type getFirstParameter(Type type) {
        if (type instanceof ParameterizedType) {
            return ((ParameterizedType) type).getActualTypeArguments()[0];
        }
        return null;
    }

    static String getChannelName(InjectionPoint injectionPoint) {
        for (Annotation qualifier : injectionPoint.getQualifiers()) {
            if (qualifier.annotationType().equals(Channel.class)) {
                return ((Channel) qualifier).value();
            }

            if (qualifier.annotationType().equals(Stream.class)) {
                return ((Stream) qualifier).value();
            }
        }
        throw new IllegalStateException("@Channel qualifier not found on + " + injectionPoint);
    }

    static Channel getChannelQualifier(InjectionPoint injectionPoint) {
        for (Annotation qualifier : injectionPoint.getQualifiers()) {
            if (qualifier.annotationType().equals(Channel.class)) {
                return (Channel) qualifier;
            }

            if (qualifier.annotationType().equals(Stream.class)) {
                return new Channel() {

                    @Override
                    public Class<? extends Annotation> annotationType() {
                        return Channel.class;
                    }

                    @Override
                    public String value() {
                        return ((Stream) qualifier).value();
                    }
                };
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object obj) {
        return (T) obj;
    }

}
