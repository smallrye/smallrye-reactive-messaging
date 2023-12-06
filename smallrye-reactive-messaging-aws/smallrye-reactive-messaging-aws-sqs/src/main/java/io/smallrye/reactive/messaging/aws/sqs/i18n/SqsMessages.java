package io.smallrye.reactive.messaging.aws.sqs.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG", length = 5)
public interface SqsMessages {

    SqsMessages msg = Messages.getBundle(SqsMessages.class);

    // TODO: ids?
    @Message(id = 19201, value = "%s is required")
    String isRequired(String fieldName);

}
