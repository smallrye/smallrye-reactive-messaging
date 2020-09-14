package io.smallrye.reactive.messaging;

import java.lang.reflect.Type;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Converter transforming {@code Message<A>} into {@code Message<B>}.
 * To register a converter, expose a, generally {@code ApplicationScoped} bean, implementing this interface.
 */
public interface MessageConverter {

    /**
     * Checks whether this instance of converter can convert the given message {@code in} into a {@code Message<T>} with
     * {@code T} being the type represented by {@code target}.
     *
     * @param in the input message, not {@code null}
     * @param target the target type, generally the type ingested by a method
     * @return {@code true} if the conversion is possible, {@code false} otherwise.
     */
    boolean accept(Message<?> in, Type target);

    /**
     * Converts the given message {@code in} into a {@code Message<T>}.
     * This method is only called after a successful call to {@link #accept(Message, Type)} with the given target type.
     *
     * @param in the input message
     * @param target the target type
     * @return the converted message.
     */
    Message<?> convert(Message<?> in, Type target);

}
