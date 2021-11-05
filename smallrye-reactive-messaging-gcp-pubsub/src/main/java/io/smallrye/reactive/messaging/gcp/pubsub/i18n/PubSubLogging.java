package io.smallrye.reactive.messaging.gcp.pubsub.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

/**
 * Logging for GCP Pub/Sub Connector
 * Assigned ID range is 14800-14899
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface PubSubLogging extends BasicLogger {

    PubSubLogging log = Logger.getMessageLogger(PubSubLogging.class, "io.smallrye.reactive.messaging.gcp.pubsub");

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 14800, value = "Topic %s already exists")
    void topicExistAlready(TopicName topic, @Cause Throwable t);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 14801, value = "Received pub/sub message %s")
    void receivedMessage(PubsubMessage message);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 14802, value = "Admin client is enabled. The GCP Connector is trying to create topics / subscriptions")
    void adminClientEnabled();
}
