package io.smallrye.reactive.messaging.eventbus.i18n;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRMSG")
public interface EventBusLogging {

    // 16800-16899 (logging)

    EventBusLogging log = Logger.getMessageLogger(EventBusLogging.class, "io.smallrye.reactive.messaging.eventbus");

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16500, value = "HTTP request POST %s  has failed with status code: %d, body is: %s")
    void postFailed(String url, int statusCode, String body);

}