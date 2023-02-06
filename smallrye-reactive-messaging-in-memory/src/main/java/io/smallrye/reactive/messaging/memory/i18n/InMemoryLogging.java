package io.smallrye.reactive.messaging.memory.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for In-memory Connector
 * Assigned ID range is 18500-18599
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface InMemoryLogging extends BasicLogger {

    InMemoryLogging log = Logger.getMessageLogger(InMemoryLogging.class, "io.smallrye.reactive.messaging.in-memory");
}
