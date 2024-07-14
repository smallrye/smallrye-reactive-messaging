package io.smallrye.reactive.messaging.aws.sqs.ack;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsAckHandler;
import io.smallrye.reactive.messaging.aws.sqs.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;

public class SqsNothingAckHandler implements SqsAckHandler {

    private final SqsAsyncClient client;
    private final Uni<String> queueUrlUni;

    public SqsNothingAckHandler(SqsAsyncClient client, Uni<String> queueUrlUni) {
        this.client = client;
        this.queueUrlUni = queueUrlUni;
    }

    @Override
    public Uni<Void> handle(SqsMessage message) {
        return queueUrlUni.map(queueUrl -> ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.getMessage().receiptHandle())
                .visibilityTimeout(0)
                .build())
                .chain(request -> Uni.createFrom().completionStage(() -> client.changeMessageVisibility(request)))
                .replaceWithVoid()
                .emitOn(message::runOnMessageContext);
    }
}
