package io.smallrye.reactive.messaging.aws.sqs.ack;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsAckHandler;
import io.smallrye.reactive.messaging.aws.sqs.SqsMessage;

public class SqsNothingAckHandler implements SqsAckHandler {

    @Override
    public Uni<Void> handle(SqsMessage message) {
        return Uni.createFrom().voidItem()
                .emitOn(message::runOnMessageContext);
    }
}
