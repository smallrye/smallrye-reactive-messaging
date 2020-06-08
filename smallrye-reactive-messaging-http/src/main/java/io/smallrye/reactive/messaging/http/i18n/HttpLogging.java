package io.smallrye.reactive.messaging.http.i18n;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRRML")
public interface HttpLogging {

    // 16500-16599 (logging)

    HttpLogging log = Logger.getMessageLogger(HttpLogging.class, "io.smallrye.reactive.messaging.http");

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16500, value = "HTTP request POST %s  has failed with status code: %d, body is: %s")
    void postFailed(String url, int statusCode, String body);

}