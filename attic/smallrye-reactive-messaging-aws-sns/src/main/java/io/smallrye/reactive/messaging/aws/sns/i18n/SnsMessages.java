package io.smallrye.reactive.messaging.aws.sns.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for AWS SNS Connector
 * Assigned ID range is 15100-15199
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface SnsMessages {

    SnsMessages msg = Messages.getBundle(SnsMessages.class);

    @Message(id = 15100, value = "Host cannot be null")
    String hostNotNull();

    @Message(id = 15101, value = "SNS Message cannot be null.")
    String messageNotNull();

}