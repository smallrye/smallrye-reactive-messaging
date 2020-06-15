package io.smallrye.reactive.messaging.cloudevents.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for Cloud event Connector
 * Assigned ID range is 15500-15599
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface CloudEventLogging extends BasicLogger {

    CloudEventLogging log = Logger.getMessageLogger(CloudEventLogging.class, "io.smallrye.reactive.messaging.cloud-events");
}
