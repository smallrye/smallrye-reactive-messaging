package io.smallrye.reactive.messaging.kafka.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for Kafka Connector
 * Assigned ID range is 18100-18199
 */
@MessageBundle(projectCode = "SRMSG")
public interface KafkaMessages {

    KafkaMessages msg = Messages.getBundle(KafkaMessages.class);

}