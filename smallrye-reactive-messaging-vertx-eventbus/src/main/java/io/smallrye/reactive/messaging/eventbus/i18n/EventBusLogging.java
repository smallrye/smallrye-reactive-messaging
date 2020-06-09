package io.smallrye.reactive.messaging.eventbus.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for EventBus Connector
 * Assigned ID range is 16800-16899
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface EventBusLogging extends BasicLogger {

    EventBusLogging log = Logger.getMessageLogger(EventBusLogging.class, "io.smallrye.reactive.messaging.eventbus");

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16500, value = "HTTP request POST %s  has failed with status code: %d, body is: %s")
    void postFailed(String url, int statusCode, String body);

}