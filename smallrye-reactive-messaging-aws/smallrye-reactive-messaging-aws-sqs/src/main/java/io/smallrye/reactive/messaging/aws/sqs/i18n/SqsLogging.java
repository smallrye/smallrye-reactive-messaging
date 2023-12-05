package io.smallrye.reactive.messaging.aws.sqs.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRMSG", length = 5)
public interface SqsLogging extends BasicLogger {

    SqsLogging log = Logger.getMessageLogger(SqsLogging.class, "io.smallrye.reactive.messaging.aws.sqs");

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19002, value = "Unable to close Sqs client")
    void unableToCloseClient(@Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 15201, value = "Shutdown in progress. Waiting for %d open requests on queue %s.")
    void shutdownProgress(int openRequests, String queue);
}
