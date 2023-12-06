package io.smallrye.reactive.messaging.aws.sqs.action;

import java.time.Duration;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsOutgoingMessage;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsOutgoingMessageMetadata;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html">AWS Documentation</a>
 */
public class SendMessageAction {

    public static Uni<Void> sendMessage(
            final SqsClientHolder<SqsConnectorOutgoingConfiguration> clientHolder, final SqsOutgoingMessage<?> message) {

        String payload = clientHolder.getSerializer().serialize(message.getPayload());

        SqsOutgoingMessageMetadata metadata = message.getMetadata(SqsOutgoingMessageMetadata.class)
                .orElseThrow(() -> new IllegalStateException("MetaData are expected to be set."));

        SendMessageRequest request = metadata.getMessageAugmenter()
                .apply(SendMessageRequest.builder()
                        .queueUrl(message.getTarget().getTargetUrl())
                        .messageBody(payload))
                .build();

        // TODO: logging
        Uni<Void> uni = Uni.createFrom().completionStage(clientHolder.getClient().sendMessage(request))
                .onItem().transformToUni(response -> {
                    OutgoingMessageMetadata.setResultOnMessage(message, response);
                    // TODO: log
                    return Uni.createFrom().completionStage(message.ack());
                });

        // TODO: configurable retry and disable sdk retry mechanism?
        if (true) {
            uni = uni.onFailure().retry()
                    .withBackOff(Duration.ofMillis(10), Duration.ofMillis(100))
                    .atMost(3);
        }

        // TODO: micrometer? Failure and Success?

        return uni;
    }
}
