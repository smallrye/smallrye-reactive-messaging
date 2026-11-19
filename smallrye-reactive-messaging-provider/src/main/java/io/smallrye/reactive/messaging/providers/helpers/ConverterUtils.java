package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getSortedInstances;

import java.lang.reflect.Type;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MessageConverter;

public class ConverterUtils {

    private ConverterUtils() {
        // Avoid direct instantiation.
    }

    public static Multi<? extends Message<?>> convert(Multi<? extends Message<?>> upstream,
            Instance<MessageConverter> converters, Type injectedPayloadType) {
        if (injectedPayloadType != null) {
            return upstream.onItem().transformToUniAndConcatenate(convertFunction(converters, injectedPayloadType));
        }
        return upstream;
    }

    /**
     * For methods consuming a specific {@link Message} type (such as a connector message implementation), run the
     * converters with that type as target for messages that are not instances of it. Messages can lose their original
     * type when they are intercepted (interceptors, observation, graceful shutdown tracking): a converter can restore it.
     * Messages that no converter handles are passed unchanged.
     *
     * @param upstream the stream of messages
     * @param converters the converters
     * @param messageType the message type consumed by the method
     * @return the stream of messages, converted to the message type when possible
     */
    public static Multi<? extends Message<?>> convertMessageType(Multi<? extends Message<?>> upstream,
            Instance<MessageConverter> converters, Class<?> messageType) {
        return upstream.onItem().transformToUniAndConcatenate(new Function<Message<?>, Uni<? extends Message<?>>>() {

            MessageConverter actual;

            @Override
            public Uni<? extends Message<?>> apply(Message<?> o) {
                if (messageType.isInstance(o)) {
                    return Uni.createFrom().item(o);
                }
                if (actual == null || !actual.canConvert(o, messageType)) {
                    actual = null;
                    for (MessageConverter conv : getSortedInstances(converters)) {
                        if (conv.canConvert(o, messageType)) {
                            actual = conv;
                            break;
                        }
                    }
                }
                if (actual == null) {
                    // No converter: keep the message as it is.
                    return Uni.createFrom().item(o);
                }
                return actual.convertUni(o, messageType)
                        .onFailure().call(t -> Uni.createFrom().completionStage(() -> o.nack(t)))
                        .onFailure().recoverWithNull();
            }
        });
    }

    public static Function<Message<?>, Uni<? extends Message<?>>> convertFunction(Instance<MessageConverter> converters,
            Type injectedPayloadType) {
        return new Function<>() {

            MessageConverter actual;

            @Override
            public Uni<? extends Message<?>> apply(Message<?> o) {
                //noinspection ConstantConditions - it can be `null`
                if (injectedPayloadType == null) {
                    return Uni.createFrom().item(o);
                } else if (o.getPayload() != null && o.getPayload().getClass().equals(injectedPayloadType)) {
                    return Uni.createFrom().item(o);
                }

                if (actual != null) {
                    // Use the cached converter.
                    return actual.convertUni(o, injectedPayloadType)
                            .onFailure().call(t -> Uni.createFrom().completionStage(() -> o.nack(t)))
                            .onFailure().recoverWithNull();
                } else {
                    if (o.getPayload() != null
                            && TypeUtils.isAssignable(o.getPayload().getClass(), injectedPayloadType)) {
                        actual = MessageConverter.IdentityConverter.INSTANCE;
                        return Uni.createFrom().item(o);
                    }
                    // Lookup and cache
                    for (MessageConverter conv : getSortedInstances(converters)) {
                        if (conv.canConvert(o, injectedPayloadType)) {
                            actual = conv;
                            return actual.convertUni(o, injectedPayloadType)
                                    .onFailure().call(t -> Uni.createFrom().completionStage(() -> o.nack(t)))
                                    .onFailure().recoverWithNull();
                        }
                    }
                    // No converter found
                    return Uni.createFrom().item(o);
                }
            }
        };
    }

}
