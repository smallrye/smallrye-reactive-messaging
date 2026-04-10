package io.smallrye.reactive.messaging.rabbitmq.reply;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Generates and parses correlation IDs for request messages
 * <p>
 * CDI-managed beans that implement this interface are discovered using
 * the {@link io.smallrye.common.annotation.Identifier} qualifier to be configured.
 *
 * @see RabbitMQRequestReply
 */
public interface CorrelationIdHandler {

    /**
     * Generates a correlation ID for the given request message.
     *
     * @param request the request message to generate the correlation ID for
     * @return the generated correlation ID
     */
    CorrelationId generate(Message<?> request);

    /**
     * Parses a correlation ID from the given string.
     *
     * @param string the string from which to parse the correlation ID
     * @return the parsed correlation ID
     */
    CorrelationId parse(String string);

}
