package io.smallrye.reactive.messaging.aws.sqs;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@ApplicationScoped
public class SqsClientProvider {

    public static SqsAsyncClient client;

    @Produces
    public SqsAsyncClient createClient() {
        return client;
    }

}
