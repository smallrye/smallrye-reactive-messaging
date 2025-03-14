package io.smallrye.reactive.messaging.aws.sns;

import jakarta.enterprise.context.ApplicationScoped;

import software.amazon.awssdk.services.sns.SnsAsyncClient;

@ApplicationScoped
public class SnsTestClientProvider implements SnsClientProvider {

    public static SnsAsyncClient client;

    @Override
    public SnsAsyncClient select(final String channel) {
        return client;
    }
}
