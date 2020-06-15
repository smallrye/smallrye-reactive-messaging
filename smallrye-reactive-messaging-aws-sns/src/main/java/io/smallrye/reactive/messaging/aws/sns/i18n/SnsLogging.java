package io.smallrye.reactive.messaging.aws.sns.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Exceptions for AWS SNS Connector
 * Assigned ID range is 15200-15299
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface SnsLogging extends BasicLogger {

    SnsLogging log = Logger.getMessageLogger(SnsLogging.class, "io.smallrye.reactive.messaging.aws-sns");

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15200, value = "Initializing Connector")
    void initializingConnector();

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 15201, value = "Error while sending the message to SNS topic %s")
    void errorSendingToTopic(String topic, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15202, value = "Message sent successfully with id %s")
    void successfullySend(String messageId);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 15203, value = "Polling message")
    void polling();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15204, value = "Topic ARN is %s, Endpoint is %s")
    void topicAndEndpointInfo(String arn, String endPoint);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15205, value = "Subscribing to topic %s with arn %s")
    void subscribingToTopic(String endPoint, String arn);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15206, value = "Message received from SNS")
    void messageReceived();

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 15207, value = "New message has been added to the queue")
    void messageAddedToQueue();

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 15208, value = "Polling message from SNS")
    void pollingMessage();

}
