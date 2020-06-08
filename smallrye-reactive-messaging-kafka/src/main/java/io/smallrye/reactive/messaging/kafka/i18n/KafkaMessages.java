package io.smallrye.reactive.messaging.kafka.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRRML")
public interface KafkaMessages {

    KafkaMessages msg = Messages.getBundle(KafkaMessages.class);

    // 18334-18666 (messaging)
}