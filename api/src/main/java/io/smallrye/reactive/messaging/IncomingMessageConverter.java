package io.smallrye.reactive.messaging;

import java.lang.reflect.Type;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;

/**
 * Converter transforming {@code Message<A>} into {@code Message<B>}.
 * To register a converter, expose a, generally {@code ApplicationScoped} bean, implementing this interface.
 * <p>
 * When multiple converters are available, implementation should override the {@link #getPriority()} method.
 * The default priority is {@link #CONVERTER_DEFAULT_PRIORITY}. Converters with higher priority are executed first.
 */
@Experimental("SmallRye only feature")
@SuppressWarnings("deprecation")
public interface IncomingMessageConverter extends AnyMessageConverter {

    /**
     * Converts the given message {@code in} into a {@code Message<T>}.
     * This method is only called after a successful call to {@link #canConvert(Message, Type)} with the given target
     * type.
     *
     * @apiNote If conversion fails, the implementation <em>must</em> handle it appropriately using {@link Message#ack()}}
     *          or {@link Message#nack(Throwable)} and perform any appropriate logging. When a conversion method returns a
     *          stream that fails, the failure and the messaging causing it will be ignored; no logging, or message
     *          acknowledgement will be performed.
     *
     * @param in the input message
     * @param target the target type
     * @return the converted message or failure in a reactive stream.
     */
    Uni<Message<?>> convert(Message<?> in, Type target);

}
