package io.smallrye.reactive.messaging.connectors.i18n;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for In-memory Connector
 * Assigned ID range is 18500-18599
 */
@MessageLogger(projectCode = "SRMSG")
public interface InMemoryLogging {

    InMemoryLogging log = Logger.getMessageLogger(InMemoryLogging.class, "io.smallrye.reactive.messaging");
}
