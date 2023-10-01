package io.smallrye.reactive.messaging.aws.sqs.action;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

/**
 * <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueUrl.html">AWS Documentation</a>
 */
public class GetQueueUrlAction {

    public static Uni<String> resolveQueueUrl(SqsClientHolder<?> clientHolder) {
        SqsConnectorCommonConfiguration config = clientHolder.getConfig();

        return Uni.createFrom().completionStage(
                clientHolder.getClient().getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(config.getQueue().orElse(config.getChannel()))
                        .queueOwnerAWSAccountId(config.getQueueResolverQueueOwnerAwsAccountId().orElse(null))
                        .build()))
                .onItem().transform(GetQueueUrlResponse::queueUrl);
    }
}
