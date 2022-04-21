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
 * The default priority is {@link #CONVERTER_DEFAULT_PRIORITY}. Converters with higher priority are executed first.
 */
@Experimental("SmallRye only feature")
public interface MessageConverter extends Prioritized {

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
     * @param target the target type, generally the type ingested by a method
     * @return {@code true} if the conversion is possible, {@code false} otherwise.
     */
    boolean canConvert(Message<?> in, Type target);

    /**
     * Converts the given message {@code in} into a {@code Message<T>}.
     * This method is only called after a successful call to {@link #canConvert(Message, Type)} with the given target type.
     *
     * @param in the input message
     * @param target the target type
     * @return the converted message.
     */
    Message<?> convert(Message<?> in, Type target);

    @Override
    default int getPriority() {
        return CONVERTER_DEFAULT_PRIORITY;
    }

    class IdentityConverter implements MessageConverter {

        public static final IdentityConverter INSTANCE = new IdentityConverter();

        private IdentityConverter() {
            // Avoid direct instantiation.
        }

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return true;
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return in;
        }
    }

}
