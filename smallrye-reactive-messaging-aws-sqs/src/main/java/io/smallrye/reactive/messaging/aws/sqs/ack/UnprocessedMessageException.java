package io.smallrye.reactive.messaging.aws.sqs.ack;

import io.vertx.core.impl.NoStackTraceThrowable;

public class UnprocessedMessageException extends NoStackTraceThrowable {

    public UnprocessedMessageException(String messageId, String queue, int seconds, int awaitingAck) {
        super(String.format("The message with id '%s from queue '%s' has waited for %d seconds to be acknowledged. " +
                "At the moment %d messages are awaiting acknowledgement.", messageId, queue, seconds, awaitingAck));
    }

}
