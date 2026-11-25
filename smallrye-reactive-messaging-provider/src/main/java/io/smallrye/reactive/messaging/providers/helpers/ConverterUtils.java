package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getSortedInstances;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

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

    public static Function<Message<?>, Uni<? extends Message<?>>> convertFunction(Instance<MessageConverter> converters,
            Type injectedPayloadType) {
        return new Function<>() {

            MessageConverter actual;

            @Override
            public Uni<? extends Message<?>> apply(Message<?> o) {
                //noinspection ConstantConditions - it can be `null`
                if (injectedPayloadType == null) {
                    return Uni.createFrom().item(o);
                }

                Object payload;
                try {
                    // The payload may be computed lazily (e.g. decoded by the connector) and fail.
                    // Throwing from this function would fail the whole stream, so nack and skip the message instead.
                    payload = o.getPayload();
                } catch (RuntimeException failure) {
                    log.unableToExtractPayload(injectedPayloadType.getTypeName(), failure);
                    return nackAndSkip(o, failure);
                }

                if (payload != null && payload.getClass().equals(injectedPayloadType)) {
                    return Uni.createFrom().item(o);
                }

                if (actual != null) {
                    // Use the cached converter.
                    return convertOrNack(actual, o, injectedPayloadType);
                } else {
                    if (payload != null && TypeUtils.isAssignable(payload.getClass(), injectedPayloadType)) {
                        actual = MessageConverter.IdentityConverter.INSTANCE;
                        return Uni.createFrom().item(o);
                    }
                    // Lookup and cache
                    for (MessageConverter conv : getSortedInstances(converters)) {
                        if (conv.canConvert(o, injectedPayloadType)) {
                            actual = conv;
                            return convertOrNack(actual, o, injectedPayloadType);
                        }
                    }
                    // No converter found
                    return Uni.createFrom().item(o);
                }
            }
        };
    }

    private static Uni<? extends Message<?>> convertOrNack(MessageConverter converter, Message<?> message, Type target) {
        return converter.convertUni(message, target)
                .onFailure().call(t -> Uni.createFrom().completionStage(() -> message.nack(t)))
                .onFailure().recoverWithNull();
    }

    /**
     * Nacks the message and emits {@code null}, so that {@code transformToUniAndConcatenate} skips it.
     * <p>
     * Like for conversion failures, a failed nack does not fail the stream: stopping the consumption
     * (e.g. with a {@code fail} failure strategy) is the responsibility of the connector.
     */
    private static Uni<? extends Message<?>> nackAndSkip(Message<?> message, Throwable failure) {
        return Uni.createFrom().completionStage(() -> message.nack(failure))
                .onItemOrFailure().transform((ignored, nackFailure) -> {
                    if (nackFailure != null) {
                        log.nackFailedAfterPayloadExtractionFailure(nackFailure);
                    }
                    return (Message<?>) null;
                });
    }

}
