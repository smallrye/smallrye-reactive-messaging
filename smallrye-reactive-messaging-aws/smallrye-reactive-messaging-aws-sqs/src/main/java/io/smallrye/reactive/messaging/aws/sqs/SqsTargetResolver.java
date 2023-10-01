package io.smallrye.reactive.messaging.aws.sqs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.action.CreateQueueAction;
import io.smallrye.reactive.messaging.aws.sqs.action.GetQueueUrlAction;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

public class SqsTargetResolver {

    private final Map<String, Uni<SqsTarget>> CACHE = new ConcurrentHashMap<>();

    public Uni<SqsTarget> resolveTarget(final SqsClientHolder<?> clientHolder) {
        final SqsConnectorCommonConfiguration config = clientHolder.getConfig();

        return CACHE.computeIfAbsent(
                clientHolder.getConfig().getQueue().orElse(config.getChannel()),
                queueName -> GetQueueUrlAction.resolveQueueUrl(clientHolder)
                        .onFailure(QueueDoesNotExistException.class)
                        .call(() -> CreateQueueAction.createQueue(clientHolder, queueName))
                        .onItem().transform(url -> new SqsTarget(queueName, url))
                        .memoize().indefinitely());
    }
}
