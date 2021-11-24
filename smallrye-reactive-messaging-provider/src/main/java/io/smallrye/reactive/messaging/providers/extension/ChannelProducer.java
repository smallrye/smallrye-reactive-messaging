package io.smallrye.reactive.messaging.providers.extension;

import static io.smallrye.reactive.messaging.providers.helpers.ConverterUtils.convert;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.Typed;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions;

/**
 * This component computes the <em>right</em> object to be injected into injection point using {@link Channel} and the
 * deprecated {@link io.smallrye.reactive.messaging.annotations.Channel}. This includes stream and emitter injections.
 */
@ApplicationScoped
public class ChannelProducer {

    @Inject
    ChannelRegistry channelRegistry;

    @Inject
    // @Any would only be needed if we wanted to allow implementations with qualifiers
    Instance<MessageConverter> converters;

    /**
     * Injects {@code Multi<Message<X>>} and {@code Multi<X>}. It also matches the injection of
     * {@code Publisher<Message<X>>} and {@code Publisher<X>}.
     *
     * @param injectionPoint the injection point
     * @param <T> the first generic parameter (either Message or X)
     * @return the Multi to be injected
     */
    @Produces
    @Typed({ Publisher.class, Multi.class })
    @Channel("") // Stream name is ignored during type-safe resolution
    <T> Multi<T> produceMulti(InjectionPoint injectionPoint) {
        Type first = getFirstParameter(injectionPoint.getType());
        if (TypeUtils.isAssignable(first, Message.class)) {
            Type payloadType = getPayloadParameterFromMessageType(first);
            if (payloadType == null) {
                return cast(getPublisher(injectionPoint));
            } else {
                return cast(convert(getPublisher(injectionPoint), converters, payloadType));
            }
        } else {
            return cast(convert(getPublisher(injectionPoint), converters, first)
                    .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                    .onItem().transform(Message::getPayload)
                    .broadcast().toAllSubscribers());
        }
    }

    /**
     * Injects {@code Multi<Message<X>>} and {@code Multi<X>}. It also matches the injection of
     * {@code Publisher<Message<X>>} and {@code Publisher<X>}.
     *
     * NOTE: this injection point is about the deprecated {@link io.smallrye.reactive.messaging.annotations.Channel} annotation.
     *
     * @param injectionPoint the injection point
     * @param <T> the first generic parameter (either Message or X)
     * @return the stream to be injected
     * @deprecated Use {@link Channel} instead.
     */
    @Produces
    @Deprecated
    @Typed({ Publisher.class, Multi.class })
    @io.smallrye.reactive.messaging.annotations.Channel("")
    <T> Multi<T> producePublisherWithLegacyChannelAnnotation(InjectionPoint injectionPoint) {
        return produceMulti(injectionPoint);
    }

    /**
     * Injects {@code PublisherBuilder<Message<X>>} and {@code PublisherBuilder<X>}
     *
     * @param injectionPoint the injection point
     * @param <T> the first generic parameter (either Message or X)
     * @return the PublisherBuilder to be injected
     */
    @Produces
    @Channel("") // Stream name is ignored during type-safe resolution
    <T> PublisherBuilder<T> producePublisherBuilder(InjectionPoint injectionPoint) {
        Multi<Object> multi = produceMulti(injectionPoint);
        return cast(ReactiveStreams.fromPublisher(multi));
    }

    /**
     * Injects {@code PublisherBuilder<Message<X>>} and {@code PublisherBuilder<X>}
     *
     * NOTE: this injection point is about the deprecated {@link io.smallrye.reactive.messaging.annotations.Channel} annotation.
     *
     * @param injectionPoint the injection point
     * @param <T> the first generic parameter (either Message or X)
     * @return the PublisherBuilder to be injected
     * @deprecated Use {@link Channel} instead.
     */
    @Produces
    @io.smallrye.reactive.messaging.annotations.Channel("") // Stream name is ignored during type-safe resolution
    <T> PublisherBuilder<T> producePublisherBuilderWithLegacyChannelAnnotation(InjectionPoint injectionPoint) {
        return producePublisherBuilder(injectionPoint);
    }

    /**
     * Injects an {@link Emitter} matching the channel name.
     *
     * @param injectionPoint the injection point
     * @param <T> the type of the emitter
     * @return the emitter
     */
    @Produces
    @Channel("") // Stream name is ignored during type-safe resolution
    <T> Emitter<T> produceEmitter(InjectionPoint injectionPoint) {
        verify(injectionPoint);
        Emitter<?> emitter = getEmitter(injectionPoint);
        return cast(emitter);
    }

    /**
     * Injects an {@link MutinyEmitter} matching the channel name.
     *
     * @param injectionPoint the injection point
     * @param <T> the type of the emitter
     * @return the emitter
     */
    @Produces
    @Channel("") // Stream name is ignored during type-safe resolution
    <T> MutinyEmitter<T> produceMutinyEmitter(InjectionPoint injectionPoint) {
        verify(injectionPoint);
        MutinyEmitter<?> emitter = getMutinyEmitter(injectionPoint);
        return cast(emitter);
    }

    /**
     * Injects an {@link io.smallrye.reactive.messaging.annotations.Emitter} (deprecated) matching the channel name.
     *
     * @param injectionPoint the injection point
     * @param <T> the type
     * @return the legacy emitter
     * @deprecated Use the new {@link Emitter} and {@link Channel} instead
     */
    @Produces
    @io.smallrye.reactive.messaging.annotations.Channel("") // Stream name is ignored during type-safe resolution
    <T> io.smallrye.reactive.messaging.annotations.Emitter<T> produceEmitterLegacy(
            InjectionPoint injectionPoint) {
        LegacyEmitterImpl<?> emitter = new LegacyEmitterImpl<>(getEmitter(injectionPoint));
        return cast(emitter);
    }

    private Multi<? extends Message<?>> getPublisher(InjectionPoint injectionPoint) {
        String name = getChannelName(injectionPoint);

        return Multi.createFrom().deferred(() -> {
            List<Publisher<? extends Message<?>>> list = channelRegistry.getPublishers(name);
            if (list.isEmpty()) {
                throw ex.illegalStateForStream(name, channelRegistry.getIncomingNames());
            } else if (list.size() == 1) {
                return Multi.createFrom().publisher(list.get(0));
            } else {
                return Multi.createBy().merging()
                        .streams(list.stream().map(p -> p).collect(Collectors.toList()));
            }
        });
    }

    private Emitter<?> getEmitter(InjectionPoint injectionPoint) {
        String name = getChannelName(injectionPoint);
        Emitter<?> emitter = channelRegistry.getEmitter(name);
        if (emitter == null) {
            throw ex.incomingNotFoundForEmitter(name);
        }
        return emitter;
    }

    private MutinyEmitter<?> getMutinyEmitter(InjectionPoint injectionPoint) {
        String name = getChannelName(injectionPoint);
        MutinyEmitter<?> emitter = channelRegistry.getMutinyEmitter(name);
        if (emitter == null) {
            throw ex.incomingNotFoundForEmitter(name);
        }
        return emitter;
    }

    private void verify(InjectionPoint injectionPoint) {
        Type type = injectionPoint.getType();
        if (type instanceof ParameterizedType && ((ParameterizedType) type).getActualTypeArguments().length > 0) {
            Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();

            if ((arguments[0] instanceof Class && arguments[0].equals(Message.class)) ||
                    (arguments[0] instanceof ParameterizedType
                            && ((ParameterizedType) arguments[0]).getRawType().equals(Message.class))) {
                throw ProviderExceptions.ex.invalidEmitterOfMessage(injectionPoint);
            }
        } else {
            throw ProviderExceptions.ex.invalidRawEmitter(injectionPoint);
        }
    }

    private Type getFirstParameter(Type type) {
        if (type instanceof ParameterizedType) {
            return ((ParameterizedType) type).getActualTypeArguments()[0];
        }
        return null;
    }

    private Type getPayloadParameterFromMessageType(Type type) {
        if (type instanceof ParameterizedType) {
            Type[] actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();
            if (actualTypeArguments.length == 1) {
                return actualTypeArguments[0];
            }
        }
        return null;
    }

    @SuppressWarnings("deprecation")
    static String getChannelName(InjectionPoint injectionPoint) {
        for (Annotation qualifier : injectionPoint.getQualifiers()) {
            if (qualifier.annotationType().equals(Channel.class)) {
                return ((Channel) qualifier).value();
            }

            if (qualifier.annotationType().equals(io.smallrye.reactive.messaging.annotations.Channel.class)) {
                return ((io.smallrye.reactive.messaging.annotations.Channel) qualifier).value();
            }
        }
        throw ex.emitterWithoutChannelAnnotation(injectionPoint);
    }

    @SuppressWarnings("deprecation")
    static Channel getChannelQualifier(InjectionPoint injectionPoint) {
        for (Annotation qualifier : injectionPoint.getQualifiers()) {
            if (qualifier.annotationType().equals(Channel.class)) {
                return (Channel) qualifier;
            }

            if (qualifier.annotationType().equals(io.smallrye.reactive.messaging.annotations.Channel.class)) {
                return new Channel() {

                    @Override
                    public Class<? extends Annotation> annotationType() {
                        return Channel.class;
                    }

                    @Override
                    public String value() {
                        return ((io.smallrye.reactive.messaging.annotations.Channel) qualifier).value();
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
