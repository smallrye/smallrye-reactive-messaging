package io.smallrye.reactive.messaging.http.i18n;

import java.util.List;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for HTTP Connector
 * Assigned ID range is 16300-16399
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface HttpExceptions {

    HttpExceptions ex = Messages.getBundle(HttpExceptions.class);

    @Message(id = 16300, value = "The `url` must be set")
    IllegalArgumentException illegalArgumentUrlNotSet();

    @Message(id = 16301, value = "Invalid HTTP Verb: %s, only PUT and POST are supported")
    IllegalArgumentException illegalArgumentInvalidVerb(String actualMethod);

    @Message(id = 16302, value = "HTTP request POST %s has not returned a valid status: %d")
    RuntimeException runtimePostInvalidStatus(String url, int statusCode);

    @Message(id = 16303, value = "Unable to find a serializer for type: %s, supported types are: %s")
    IllegalArgumentException unableToFindSerializer(Class payloadClass, List<String> supportedTypes);

    @Message(id = 16304, value = "Unable to load the class %s or unable to instantiate it")
    IllegalArgumentException unableToLoadClass(String className);

}
