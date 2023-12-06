package io.smallrye.reactive.messaging.aws.sqs.action;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsIncomingMessage;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

public class DeleteMessageAction {

    public static Uni<Void> deleteMessage(
            SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder, SqsIncomingMessage<?> message) {

        final DeleteMessageRequest request = DeleteMessageRequest.builder().queueUrl(message.getTarget().getTargetUrl())
                .receiptHandle(message.getSqsMetadata().getAwsMessage().receiptHandle()).build();

        return Uni.createFrom().completionStage(clientHolder.getClient().deleteMessage(request)).replaceWithVoid();
    }
}
