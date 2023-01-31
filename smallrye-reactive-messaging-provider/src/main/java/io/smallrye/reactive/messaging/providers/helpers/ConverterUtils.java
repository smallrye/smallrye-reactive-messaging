package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getSortedInstances;

import java.lang.reflect.Type;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MessageConverter;

public class ConverterUtils {

    private ConverterUtils() {
        // Avoid direct instantiation.
    }

    public static Multi<? extends Message<?>> convert(Multi<? extends Message<?>> upstream,
            Instance<MessageConverter> converters, Type injectedPayloadType) {
        if (injectedPayloadType != null) {
            return upstream
                    .map(new Function<Message<?>, Message<?>>() {

                        MessageConverter actual;

                        @Override
                        public Message<?> apply(Message<?> o) {
                            //noinspection ConstantConditions - it can be `null`
                            if (injectedPayloadType == null) {
                                return o;
                            } else if (o.getPayload() != null && o.getPayload().getClass().equals(injectedPayloadType)) {
                                return o;
                            }

                            if (actual != null) {
                                // Use the cached converter.
                                return actual.convert(o, injectedPayloadType);
                            } else {
                                if (o.getPayload() != null
                                        && TypeUtils.isAssignable(o.getPayload().getClass(), injectedPayloadType)) {
                                    actual = MessageConverter.IdentityConverter.INSTANCE;
                                    return o;
                                }
                                // Lookup and cache
                                for (MessageConverter conv : getSortedInstances(converters)) {
                                    if (conv.canConvert(o, injectedPayloadType)) {
                                        actual = conv;
                                        return actual.convert(o, injectedPayloadType);
                                    }
                                }
                                // No converter found
                                return o;
                            }
                        }
                    });
        }
        return upstream;
    }

}
