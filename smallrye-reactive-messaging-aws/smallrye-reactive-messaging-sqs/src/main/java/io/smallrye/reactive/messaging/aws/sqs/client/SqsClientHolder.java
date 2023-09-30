package io.smallrye.reactive.messaging.aws.sqs.client;

import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.cache.TargetCache;
import io.smallrye.reactive.messaging.json.JsonMapping;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class SqsClientHolder<C extends SqsConnectorCommonConfiguration> {
    private final SqsAsyncClient client;
    private final C config;
    private final JsonMapping jsonMapping;
    private final TargetCache targetCache;

    public SqsClientHolder(SqsAsyncClient client, C config, JsonMapping jsonMapping, TargetCache targetCache) {
        this.client = client;
        this.config = config;
        this.jsonMapping = jsonMapping;
        this.targetCache = targetCache;
    }

    public SqsAsyncClient getClient() {
        return client;
    }

    public C getConfig() {
        return config;
    }

    public JsonMapping getJsonMapping() {
        return jsonMapping;
    }

    public TargetCache getTargetCache() {
        return targetCache;
    }
}
