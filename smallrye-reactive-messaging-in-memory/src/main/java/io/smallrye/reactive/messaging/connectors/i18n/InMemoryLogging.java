package io.smallrye.reactive.messaging.connectors.i18n;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRRML")
public interface InMemoryLogging {

    InMemoryLogging log = Logger.getMessageLogger(InMemoryLogging.class, "io.smallrye.reactive.messaging");

    // 19667-19999 (logging)
}