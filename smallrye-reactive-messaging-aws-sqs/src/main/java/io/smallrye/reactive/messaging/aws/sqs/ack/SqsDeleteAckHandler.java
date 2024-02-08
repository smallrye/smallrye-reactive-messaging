package io.smallrye.reactive.messaging.aws.sqs.ack;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsAckHandler;
import io.smallrye.reactive.messaging.aws.sqs.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

public class SqsDeleteAckHandler implements SqsAckHandler {

    private final SqsAsyncClient client;
    private final Uni<String> queueUrlUni;

    public SqsDeleteAckHandler(SqsAsyncClient client, Uni<String> queueUrlUni) {
        this.client = client;
        this.queueUrlUni = queueUrlUni;
    }

    @Override
    public Uni<Void> handle(SqsMessage message) {
        return queueUrlUni.map(queueUrl -> DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.getMessage().receiptHandle())
                .build())
                .chain(request -> Uni.createFrom().completionStage(() -> client.deleteMessage(request)))
                .replaceWithVoid()
                .emitOn(message::runOnMessageContext);
    }
}
