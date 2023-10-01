package io.smallrye.reactive.messaging.aws.sqs.action;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.SqsTarget;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html">AWS Documentation</a>
 */
public class ReceiveMessageAction {

    public static <T extends Message<?>> Uni<T> receiveMessages(
            SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder,
            SqsTarget target) {

        final ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(target.getTargetUrl())

                .build();

        return null;
    }
}
