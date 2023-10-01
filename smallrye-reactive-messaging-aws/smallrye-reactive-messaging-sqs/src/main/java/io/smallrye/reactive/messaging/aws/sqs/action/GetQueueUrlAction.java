package io.smallrye.reactive.messaging.aws.sqs.action;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessage;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessageMetadata;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

public class GetQueueUrlAction {

    public static <M extends SqsMessageMetadata> Uni<String> resolveQueueUrl(
            SqsClientHolder<?> clientHolder, SqsMessage<?, M> message) {
        SqsConnectorCommonConfiguration config = clientHolder.getConfig();
        SqsMessageMetadata sqsMetadata = message.getSqsMetadata();

        return Uni.createFrom().completionStage(
                clientHolder.getClient().getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(config.getQueue().orElse(config.getChannel()))
                        .queueOwnerAWSAccountId(sqsMetadata.getQueueOwnerAWSAccountId())
                        .build()))
                .onItem().transform(GetQueueUrlResponse::queueUrl);
    }
}
