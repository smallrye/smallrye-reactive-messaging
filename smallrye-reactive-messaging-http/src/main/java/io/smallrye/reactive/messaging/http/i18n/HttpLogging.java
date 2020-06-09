package io.smallrye.reactive.messaging.http.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for HTTP Connector
 * Assigned ID range is 16500-16599
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface HttpLogging extends BasicLogger {

    HttpLogging log = Logger.getMessageLogger(HttpLogging.class, "io.smallrye.reactive.messaging.http");

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16500, value = "HTTP request POST %s  has failed with status code: %d, body is: %s")
    void postFailed(String url, int statusCode, String body);

}