package io.smallrye.reactive.messaging.aws.sns.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface SnsExceptions {

    SnsExceptions ex = Messages.getBundle(SnsExceptions.class);

    // 15000-15099 (exceptions)

    @Message(id = 15000, value = "Invalid URL")
    IllegalArgumentException illegalArgumentInvalidURL(@Cause Throwable t);

}
