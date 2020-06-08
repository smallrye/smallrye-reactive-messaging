package io.smallrye.reactive.messaging.camel.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

import io.smallrye.reactive.messaging.camel.CamelFailureHandler;

@MessageBundle(projectCode = "SRMSG")
public interface CamelExceptions {

    CamelExceptions ex = Messages.getBundle(CamelExceptions.class);

    // 17600-17699 (exceptions)

    @Message(id = 17600, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownStrategy(CamelFailureHandler.Strategy strategy);

}
