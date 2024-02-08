package io.smallrye.reactive.messaging.aws.sqs.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for AWS Sqs Connector
 * Assigned ID range is 19400-19499
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface AwsSqsExceptions {

    AwsSqsExceptions ex = Messages.getBundle(AwsSqsExceptions.class);

    @Message(id = 19400, value = "Failed to retrieve AWS Sqs queue url")
    IllegalStateException illegalStateUnableToRetrieveQueueUrl(@Cause Throwable t);

    @Message(id = 19401, value = "Unable to load the class %s")
    IllegalStateException illegalStateUnableToLoadClass(String className, @Cause Throwable t);
}
