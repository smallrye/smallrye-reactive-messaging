package io.smallrye.reactive.messaging.aws.sqs.action;

import java.util.List;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.SqsTarget;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html">AWS Documentation</a>
 */
public class ReceiveMessageAction {

    public static Uni<ReceiveMessageResponse> receiveMessages(
            SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder,
            SqsTarget target, List<String> attributeNames, List<String> messageAttributeNames) {

        final SqsConnectorIncomingConfiguration config = clientHolder.getConfig();

        final ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(target.getTargetUrl())
                .maxNumberOfMessages(config.getMaxNumberOfMessages())
                .waitTimeSeconds(config.getWaitTimeSeconds())
                .visibilityTimeout(config.getVisibilityTimeout())
                .attributeNamesWithStrings(attributeNames)
                .messageAttributeNames(messageAttributeNames)
                .build();

        return Uni.createFrom().completionStage(clientHolder.getClient().receiveMessage(request));
    }

}
