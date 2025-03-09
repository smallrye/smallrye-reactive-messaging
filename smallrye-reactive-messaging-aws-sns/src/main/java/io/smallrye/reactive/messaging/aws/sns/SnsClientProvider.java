package io.smallrye.reactive.messaging.aws.sns;

import software.amazon.awssdk.services.sns.SnsAsyncClient;

public interface SnsClientProvider {
    SnsAsyncClient select(String channel);
}
