package io.smallrye.reactive.messaging.aws.sns;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import software.amazon.awssdk.services.sns.SnsAsyncClient;

@ApplicationScoped
public class SnsTestClientProvider {

    public static SnsAsyncClient client;

    @Produces
    public SnsAsyncClient createClient() {
        return client;
    }
}
