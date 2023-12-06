package io.smallrye.reactive.messaging.aws.sqs.action;

import static io.smallrye.reactive.messaging.aws.config.ConfigHelper.parseToMap;

import java.util.HashMap;
import java.util.Map;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.vertx.core.json.JsonObject;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * <a href=
 * "https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html">AWS
 * Documentation</a>
 */
public class CreateQueueAction {

    public static Uni<String> createQueue(SqsClientHolder<?> clientHolder, String queueName) {
        SqsConnectorCommonConfiguration config = clientHolder.getConfig();

        if (Boolean.FALSE.equals(config.getCreateQueueEnabled())) {
            return Uni.createFrom().nullItem();
        }

        Uni<Map<String, String>> uni = Uni.createFrom().item(Map.of());

        if (Boolean.TRUE.equals(config.getCreateQueueDeadLetterQueueEnabled())) {
            // Parsing is slow. But we assume that creating queues does not happen that often.
            final Map<String, String> attributes = parseToMap(config.getCreateQueueDeadLetterQueueAttributes());
            final Map<String, String> tags = parseToMap(config.getCreateQueueDeadLetterQueueTags());

            uni = uni.onItem().transformToUni(ignore -> createDlq(
                    clientHolder, queueName, config,
                    attributes, tags));
        }

        return uni.onItem().transformToUni(preparedAttributes -> {
            // Parsing is slow. But we assume that creating queues does not happen that often.
            final Map<String, String> configAttributes = parseToMap(config.getCreateQueueAttributes());
            final Map<String, String> tags = parseToMap(config.getCreateQueueTags());
            final Map<String, String> attributes = new HashMap<>(preparedAttributes.size() + configAttributes.size());

            attributes.putAll(preparedAttributes);
            attributes.putAll(configAttributes);

            return createQueue(clientHolder, queueName, attributes, tags);
        }).onItem().transform(CreateQueueResponse::queueUrl);
    }

    private static Uni<Map<String, String>> createDlq(
            SqsClientHolder<?> clientHolder, String queueName,
            SqsConnectorCommonConfiguration config,
            Map<String, String> attributes,
            Map<String, String> tags) {

        Uni<CreateQueueResponse> uni = createQueue(clientHolder,
                config.getCreateQueueDeadLetterQueuePrefix() + queueName + config.getCreateQueueDeadLetterQueueSuffix(),
                attributes, tags);

        return uni.onItem().transformToUni(response -> getQueueArn(clientHolder, response))
                .onItem().transform(arn -> Map.of(
                        QueueAttributeName.REDRIVE_POLICY.toString(), createRedrivePolicy(config, arn)));
    }

    private static String createRedrivePolicy(SqsConnectorCommonConfiguration config,
            String arn) {

        final JsonObject data = JsonObject.of("deadLetterTargetArn", arn)
                .put("maxReceiveCount", config.getCreateQueueDeadLetterQueueMaxReceiveCount());
        return data.toString();
    }

    private static Uni<CreateQueueResponse> createQueue(
            SqsClientHolder<?> clientHolder, String queueName,
            Map<String, String> attributes,
            Map<String, String> tags) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributesWithStrings(attributes)
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
