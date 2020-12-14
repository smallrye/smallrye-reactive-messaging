package io.smallrye.reactive.messaging.amqp.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for AMQP Connector
 * Assigned ID range is 16100-16199
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface AMQPMessages {

    AMQPMessages msg = Messages.getBundle(AMQPMessages.class);

}
