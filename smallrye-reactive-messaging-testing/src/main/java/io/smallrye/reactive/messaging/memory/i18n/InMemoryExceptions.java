package io.smallrye.reactive.messaging.memory.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for In-memory Connector
 * Assigned ID range is 18300-18399
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface InMemoryExceptions {

    InMemoryExceptions ex = Messages.getBundle(InMemoryExceptions.class);

    @Message(id = 18300, value = "The channel name must not be `null` or blank")
    IllegalArgumentException illegalArgumentChannelNameNull();

    @Message(id = 18301, value = "Invalid incoming configuration, `channel-name` is not set")
    IllegalArgumentException illegalArgumentInvalidIncomingConfig();

    @Message(id = 18302, value = "Invalid outgoing configuration, `channel-name` is not set")
    IllegalArgumentException illegalArgumentInvalidOutgoingConfig();

    @Message(id = 18303, value = "`channel` must not be `null`")
    IllegalArgumentException illegalArgumentChannelMustNotBeNull();

    @Message(id = 18304, value = "Unknown channel %s")
    IllegalArgumentException illegalArgumentUnknownChannel(String channel);

}
