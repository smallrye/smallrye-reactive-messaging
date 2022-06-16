package io.smallrye.reactive.messaging.providers.helpers;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.AnyMessageConverter;
import io.smallrye.reactive.messaging.IncomingMessageConverter;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;

public class ConverterUtils {

    private ConverterUtils() {
        // Avoid direct instantiation.
    }

    @SuppressWarnings("deprecation")
    public static Multi<? extends Message<?>> convert(Multi<? extends Message<?>> upstream,
            Instance<AnyMessageConverter> converters, Type injectedPayloadType) {
        if (injectedPayloadType == null) {
            return upstream;
        }
        return upstream.onItem().transformToMultiAndConcatenate(new Function<Message<?>, Multi<? extends Message<?>>>() {

            @Override
            public Multi<? extends Message<?>> apply(Message<?> message) {
                if (canSkipConversion(message, injectedPayloadType)) {
                    return noConversion(message);
                }

                AnyMessageConverter converter = lookupConverter(message, injectedPayloadType);
                if (converter == null) {
                    ProviderLogging.log.noConverterFound(injectedPayloadType);
                    return noConversion(message);
                }

                // Use the cached converter.
                return convert(converter, message, injectedPayloadType);
            }

            AnyMessageConverter lastUsedConverter;

            /**
             * Looks up a compatible converter.
             *
             * The last used converter is preferred, but it is checked against the specific message
             * instance being processed. If the last used converter cannot convert this message a
             * new converter is looked up manually and saved as the new last used converter.
             *
             * @param message Message being converted
             * @param targetPayloadType Target type of payload the message payload must be converted to
             * @return Compatible converter or null
             */
            private AnyMessageConverter lookupConverter(Message<?> message, Type targetPayloadType) {
                // Check if we can use the last used converter...
                if (lastUsedConverter != null && lastUsedConverter.canConvert(message, targetPayloadType)) {
                    return lastUsedConverter;
                }
                // Lookup and save a new compatible converter
                for (AnyMessageConverter conv : getSortedConverters(converters)) {
                    if (conv.canConvert(message, injectedPayloadType)) {
                        lastUsedConverter = conv;
                        return lastUsedConverter;
                    }
                }
                return null;
            }

        });
    }

    @SuppressWarnings("deprecation")
    private static Multi<? extends Message<?>> convert(AnyMessageConverter converter, Message<?> message,
            Type targetPayloadType) {
        if (converter instanceof MessageConverter) {
            MessageConverter messageConverter = (MessageConverter) converter;
            return Multi.createFrom().item(message)
                    .map(m -> messageConverter.convert(m, targetPayloadType))
                    .onFailure().recoverWithMulti(ex -> {
                        // Automatically ack the message after logging the error because the message
                        // converter cannot do it itself due it's limited interface.
                        ProviderLogging.log.deprecatedConversionFailed(messageConverter.getClass().getSimpleName(), ex);
                        return Multi.createFrom().completionStage(message.nack(ex))
                                .flatMap(i -> Multi.createFrom().empty());
                    });
        } else {
            IncomingMessageConverter incomingMessageConverter = (IncomingMessageConverter) converter;
            return incomingMessageConverter.convert(message, targetPayloadType)
                    .toMulti().onFailure().recoverWithMulti(() -> {
                        // Ignore conversion failures since the interface _requires_ they handle
                        // failures, including calling ack/nack.
                        return Multi.createFrom().empty();
                    });
        }
    }

    private static boolean canSkipConversion(Message<?> message, Type target) {
        Object payload = message.getPayload();
        return payload != null && TypeUtils.isAssignable(payload.getClass(), target);
    }

    private static Multi<? extends Message<?>> noConversion(Message<?> message) {
        return Multi.createFrom().item(message);
    }

    @SuppressWarnings("deprecation")
    private static List<AnyMessageConverter> getSortedConverters(Instance<AnyMessageConverter> converters) {
        if (converters.isUnsatisfied()) {
            return Collections.emptyList();
        }

        // NOSONAR
        return converters.stream().sorted(Comparator.comparingInt(AnyMessageConverter::getPriority))
                .collect(Collectors.toList());
    }
}
