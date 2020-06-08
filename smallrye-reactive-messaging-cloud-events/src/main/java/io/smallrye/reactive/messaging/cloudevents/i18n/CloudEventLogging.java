package io.smallrye.reactive.messaging.cloudevents.i18n;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRMSG")
public interface CloudEventLogging {

    // 15500-15599 (logging)

    CloudEventLogging log = Logger.getMessageLogger(CloudEventLogging.class, "io.smallrye.reactive.messaging.cloudevents");

}