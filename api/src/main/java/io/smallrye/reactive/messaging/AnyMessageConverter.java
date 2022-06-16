package io.smallrye.reactive.messaging;

import java.lang.reflect.Type;

import javax.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Base interface for synchronous and reactive message converters.
 *
 * NOTE: Intended for code evolution only! After MessageConverter has had a
 * period of deprecation it, along with this interface, with be removed.
 *
 * @see IncomingMessageConverter
 * @deprecated Use {@link IncomingMessageConverter}.
 */
@Deprecated
public interface AnyMessageConverter extends Prioritized {

    /**
     * Default priority: {@code 100}
     */
    int CONVERTER_DEFAULT_PRIORITY = 100;

    @Override
    default int getPriority() {
        return CONVERTER_DEFAULT_PRIORITY;
    }

    /**
     * Checks whether this instance of converter can convert the given message {@code in} into a {@code Message<T>} with
     * {@code T} being the type represented by {@code target}.
     *
     * When reactive messaging looks for a converter, it picks the first converter returning {@code true} for a given
     * message.
     *
     * @param in the input message, not {@code null}
     * @param target the target type, generally the type ingested by a method
     * @return {@code true} if the conversion is possible, {@code false} otherwise.
     */
    boolean canConvert(Message<?> in, Type target);

}
