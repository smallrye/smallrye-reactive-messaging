package io.smallrye.reactive.messaging.aws.sqs;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.action.CreateQueueAction;
import io.smallrye.reactive.messaging.aws.sqs.action.GetQueueUrlAction;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessage;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessageMetadata;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SqsTargetResolver {

    private final Map<String, Uni<SqsTarget>> CACHE = new ConcurrentHashMap<>();

    public <M extends SqsMessageMetadata> Uni<SqsTarget> getTarget(
            final SqsClientHolder<?> clientHolder, final SqsMessage<?, M> message) {
        final SqsConnectorCommonConfiguration config = clientHolder.getConfig();

        return CACHE.computeIfAbsent(
                clientHolder.getConfig().getQueue().orElse(config.getChannel()),
                key -> GetQueueUrlAction.resolveQueueUrl(clientHolder, message)
                        .onFailure(QueueDoesNotExistException.class)
                        .call(() -> CreateQueueAction.createQueue(clientHolder, message))
                        .onItem().transform(url -> new SqsTarget(key, url))
                        .memoize().indefinitely());
    }
}
