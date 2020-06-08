package io.smallrye.reactive.messaging.connectors.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRRML")
public interface InMemoryExceptions {

    InMemoryExceptions ex = Messages.getBundle(InMemoryExceptions.class);

    // 19000-19333 (exceptions)

    @Message(id = 19000, value = "The channel name must not be `null` or blank")
    IllegalArgumentException illegalArgumentChannelNameNull();

    @Message(id = 19001, value = "Invalid incoming configuration, `channel-name` is not set")
    IllegalArgumentException illegalArgumentInvalidIncomingConfig();

    @Message(id = 19002, value = "Invalid outgoing configuration, `channel-name` is not set")
    IllegalArgumentException illegalArgumentInvalidOutgoingConfig();

    @Message(id = 19003, value = "`channel` must not be `null`")
    IllegalArgumentException illegalArgumentChannelMustNotBeNull();

    @Message(id = 19004, value = "Unknown channel %s")
    IllegalArgumentException illegalArgumentUnknownChannel(String channel);

}
