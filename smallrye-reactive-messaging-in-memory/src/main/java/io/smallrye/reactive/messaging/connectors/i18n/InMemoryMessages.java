package io.smallrye.reactive.messaging.connectors.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRRML")
public interface InMemoryMessages {

    InMemoryMessages msg = Messages.getBundle(InMemoryMessages.class);

    // 19334-19666 (messaging)
}