package io.smallrye.reactive.messaging.camel.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

import io.smallrye.reactive.messaging.camel.CamelFailureHandler;

/**
 * Exceptions for Camel Connector
 * Assigned ID range is 17600-17699
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface CamelExceptions {

    CamelExceptions ex = Messages.getBundle(CamelExceptions.class);

    @Message(id = 17600, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownStrategy(CamelFailureHandler.Strategy strategy);

    @Message(id = 17601, value = "Unable to register CamelReactiveStreamsService service")
    IllegalStateException unableToRegisterService(@Cause Throwable cause);

}
