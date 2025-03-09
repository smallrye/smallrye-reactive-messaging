package io.smallrye.reactive.messaging.aws.sns.i18n;

import static java.lang.invoke.MethodHandles.lookup;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRMSG", length = 5)
public interface AwsSnsLogging {

    AwsSnsLogging log = Logger.getMessageLogger(lookup(), AwsSnsLogging.class, "io.smallrye.reactive.messaging.aws.sns");

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 19503, value = "Failed to load the AWS credentials provider, using the default credential provider chain %s")
    void failedToLoadAwsCredentialsProvider(String message);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 19504, value = "Topic arn for channel %s : %s")
    void topicArnForChannel(String channel, String topicArn);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 19505, value = "Message sent for channel %s with message id %s and sequence number %s")
    void messageSentToChannel(String channel, String messageId, String sequence);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 19506, value = "Error while sending message from channel '%s'")
    void unableToDispatch(String channel, @Cause Throwable e);
}
