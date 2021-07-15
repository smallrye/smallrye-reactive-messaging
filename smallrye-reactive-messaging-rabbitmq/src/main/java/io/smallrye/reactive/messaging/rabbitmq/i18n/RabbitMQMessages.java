package io.smallrye.reactive.messaging.rabbitmq.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for RabbitMQ Connector
 * Assigned ID range is 16100-16199
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface RabbitMQMessages {

    RabbitMQMessages msg = Messages.getBundle(RabbitMQMessages.class);

}
