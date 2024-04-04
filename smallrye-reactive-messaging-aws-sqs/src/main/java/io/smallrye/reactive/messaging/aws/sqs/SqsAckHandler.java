package io.smallrye.reactive.messaging.aws.sqs;

import io.smallrye.mutiny.Uni;

public interface SqsAckHandler {

    Uni<Void> handle(SqsMessage message);

}
