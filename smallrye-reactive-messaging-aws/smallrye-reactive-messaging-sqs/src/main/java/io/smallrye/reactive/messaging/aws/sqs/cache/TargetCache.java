package io.smallrye.reactive.messaging.aws.sqs.cache;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.Target;
import io.smallrye.reactive.messaging.aws.sqs.action.GetQueueUrlAction;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessage;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessageMetadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TargetCache {

    private final Map<String, Uni<Target>> CACHE = new ConcurrentHashMap<>();

    public <M extends SqsMessageMetadata> Uni<Target> getTarget(
            final SqsClientHolder<?> clientHolder, final SqsMessage<?, M> message) {
        final SqsConnectorCommonConfiguration config = clientHolder.getConfig();

        return CACHE.computeIfAbsent(
                clientHolder.getConfig().getQueue().orElse(config.getChannel()),
                key -> GetQueueUrlAction.resolveQueueUrl(clientHolder, message)
                        .onItem().transform(url -> new Target(key, url))
                        .memoize().indefinitely());
    }
}
