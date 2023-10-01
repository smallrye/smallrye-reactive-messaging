package io.smallrye.reactive.messaging.aws.sqs.client;

import io.smallrye.reactive.messaging.aws.client.ClientHolder;
import io.smallrye.reactive.messaging.aws.serialization.Deserializer;
import io.smallrye.reactive.messaging.aws.serialization.Serializer;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.SqsTargetResolver;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class SqsClientHolder<C extends SqsConnectorCommonConfiguration> extends ClientHolder<SqsAsyncClient, C> {
    private final SqsTargetResolver targetResolver;

    public SqsClientHolder(SqsAsyncClient client, Vertx vertx, C config, SqsTargetResolver targetResolver,
            Serializer serializer, Deserializer deserializer) {
        super(client, vertx, config, serializer, deserializer);
        this.targetResolver = targetResolver;
    }

    public SqsTargetResolver getTargetCache() {
        return targetResolver;
    }
}
