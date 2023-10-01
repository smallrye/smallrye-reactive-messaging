package io.smallrye.reactive.messaging.aws.sqs.action;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsCreateQueueMetadata;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessage;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessageMetadata;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Map;

/**
 * <a href=
 * "https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html#SQS-CreateQueue-request-attributes">AWS
 * Documentation</a>
 */
public class CreateQueueAction {

    public static <M extends SqsMessageMetadata> Uni<String> createQueue(
            SqsClientHolder<?> clientHolder, SqsMessage<?, M> message) {
        SqsConnectorCommonConfiguration config = clientHolder.getConfig();

        if (!config.getCreateQueueEnabled()) {
            return Uni.createFrom().nullItem();
        }

        String queueName = message.getTarget().getTargetName();
        Uni<Map<QueueAttributeName, String>> uni = Uni.createFrom().nullItem();
        M metadata = message.getSqsMetadata();

        if (config.getCreateQueueDlqEnabled()) {
            final SqsCreateQueueMetadata createQueueDlqMetadata = metadata.getCreateQueueDlqMetadata();
            uni = uni.onItem().transformToUni(ignore -> createDlq(
                    clientHolder, queueName, config,
                    createQueueDlqMetadata.getAttributes(), createQueueDlqMetadata.getTags()
            ));
        }


        CreateQueueRequest.builder().queueName(queueName);

        return uni.replaceWith("");
    }

    private static Uni<Map<QueueAttributeName, String>> createDlq(SqsClientHolder<?> clientHolder, String queueName,
                                                                  SqsConnectorCommonConfiguration config,
                                                                  Map<QueueAttributeName, String> attributes,
                                                                  Map<String, String> tags) {

        Uni<CreateQueueResponse> uni = createQueue(clientHolder,
                config.getCreateQueueDlqPrefix() + queueName + config.getCreateQueueDlqSuffix(),
                Map.of(), Map.of());

        return uni.onItem().transformToUni(response -> getQueueArn(clientHolder, response))
                .onItem().transform(arn -> Map.of(
                        QueueAttributeName.REDRIVE_POLICY, createRedrivePolicy(clientHolder, config, arn)));
    }

    private static String createRedrivePolicy(SqsClientHolder<?> clientHolder, SqsConnectorCommonConfiguration config,
                                              String arn) {
        return clientHolder.getJsonMapping().toJson(Map.of(
                "deadLetterTargetArn", arn,
                "maxReceiveCount", config.getCreateQueueDlqMaxReceiveCount()
        ));
    }

    private static Uni<CreateQueueResponse> createQueue(SqsClientHolder<?> clientHolder, String queueName,
                                                        Map<QueueAttributeName, String> attributes,
                                                        Map<String, String> tags) {

        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(attributes)
                .tags(tags)
                .build();

        return Uni.createFrom().completionStage(clientHolder.getClient().createQueue(request))
                // TODO: Log exception and success
                .onFailure().invoke(e -> {
                });
    }

    private static Uni<String> getQueueArn(SqsClientHolder<?> clientHolder, CreateQueueResponse response) {

        final GetQueueAttributesRequest request = GetQueueAttributesRequest.builder().queueUrl(response.queueUrl())
                .attributeNames(QueueAttributeName.QUEUE_ARN).build();

        return Uni.createFrom().completionStage(clientHolder.getClient().getQueueAttributes(request))
                .onItem().transform(r -> r.attributes().get(QueueAttributeName.QUEUE_ARN));
    }
}
