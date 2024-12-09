package io.smallrye.reactive.messaging.aws.sqs;

import java.util.function.BiConsumer;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public interface SqsFailureHandler {

    interface Strategy {
        String DELETE = "delete";
        String VISIBILITY = "visibility";
        String FAIL = "fail";
        String IGNORE = "ignore";
    }

    interface Factory {
        SqsFailureHandler create(String channel, SqsAsyncClient client, Uni<String> queueUrlUni,
                BiConsumer<Throwable, Boolean> reportFailure);
    }

    Uni<Void> handle(SqsMessage<?> message, Metadata metadata, Throwable throwable);
}
