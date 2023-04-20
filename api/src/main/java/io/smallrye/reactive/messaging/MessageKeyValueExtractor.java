package io.smallrye.reactive.messaging;

import java.lang.reflect.Type;

import jakarta.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;

/**
 * Converter transforming {@code Message<A>} into {@code Message<B>}.
 * To register a converter, expose a, generally {@code ApplicationScoped} bean, implementing this interface.
 * <p>
 * When multiple converters are available, implementation should override the {@link #getPriority()} method.
 * Converters with higher priority (lesser value) are executed first.
 * The default priority is {@link #CONVERTER_DEFAULT_PRIORITY}.
 */
@Experimental("SmallRye only feature")
public interface MessageKeyValueExtractor extends Prioritized {

    /**
     * Default priority: {@code 100}
     */
    int CONVERTER_DEFAULT_PRIORITY = 100;

    /**
     * Checks whether this instance of converter can convert the given message {@code in} into a {@code Message<T>} with
     * {@code T} being the type represented by {@code target}.
     *
     * When reactive messaging looks for a converter, it picks the first converter returning {@code true} for a given
     * message.
     *
     * @param in the input message, not {@code null}
     * @param keyType the key target type
     * @param valueType the value target type
     * @return {@code true} if the conversion is possible, {@code false} otherwise.
     */
    boolean canExtract(Message<?> in, Type keyType, Type valueType);

    /**
     * Extracts the key from the given message {@code in}.
     * This method is only called after a successful call to {@link #canExtract(Message, Type, Type)} with the given target
     * type.
     *
     * @param in the input message
     * @param keyType the target type
     * @return the converted message.
     */
    Object extractKey(Message<?> in, Type keyType);

    /**
     * Extracts the value from the given message {@code in}.
     * This method is only called after a successful call to {@link #canExtract(Message, Type, Type)} with the given target
     * type.
     *
     * @param in the input message
     * @param valueType the target type
     * @return the converted message.
     */
    Object extractValue(Message<?> in, Type valueType);

    @Override
    default int getPriority() {
        return CONVERTER_DEFAULT_PRIORITY;
    }

}
