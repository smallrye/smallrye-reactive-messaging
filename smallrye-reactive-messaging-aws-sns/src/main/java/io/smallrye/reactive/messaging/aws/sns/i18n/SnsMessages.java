package io.smallrye.reactive.messaging.aws.sns.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRRML")
public interface SnsMessages {

    SnsMessages msg = Messages.getBundle(SnsMessages.class);

    // 15100-15199 (String messages)

    @Message(id = 15100, value = "Host cannot be null")
    String hostNotNull();

    @Message(id = 15101, value = "SNS Message cannot be null.")
    String messageNotNull();

}