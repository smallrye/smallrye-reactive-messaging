package io.smallrye.reactive.messaging.aws.sns.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for AWS SNS Connector
 * Assigned ID range is 15000-15099
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface SnsExceptions {

    SnsExceptions ex = Messages.getBundle(SnsExceptions.class);

    @Message(id = 15000, value = "Invalid URL")
    IllegalArgumentException illegalArgumentInvalidURL(@Cause Throwable t);

}
