package io.smallrye.reactive.messaging.aws.sqs.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.Logger.Level;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for AWS Sqs Connector
 * Assigned ID range is 19300-19399
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface AwsSqsLogging extends BasicLogger {
    AwsSqsLogging log = Logger.getMessageLogger(AwsSqsLogging.class, "io.smallrye.reactive.messaging.aws.sqs");

    @LogMessage(level = Level.TRACE)
    @Message(id = 19300, value = "Received Aws Sqs message %s")
    void receivedMessage(String message);

    @LogMessage(level = Level.WARN)
    @Message(id = 19301, value = "Aws Sqs message is null")
    void receivedEmptyMessage();

    @LogMessage(level = Level.ERROR)
    @Message(id = 19302, value = "Error while receiving the message %e")
    void errorReceivingMessage(String message);

    @LogMessage(level = Level.WARN)
    @Message(id = 19303, value = "Failed to load the AWS credential loader, use the default credential provider chain %s")
    void failedToLoadAwsCredentialLoader(String message);
}
