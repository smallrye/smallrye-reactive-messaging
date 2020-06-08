package io.smallrye.reactive.messaging.camel.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface CamelMessages {

    CamelMessages msg = Messages.getBundle(CamelMessages.class);

    // 17700-17799 (messaging)
}