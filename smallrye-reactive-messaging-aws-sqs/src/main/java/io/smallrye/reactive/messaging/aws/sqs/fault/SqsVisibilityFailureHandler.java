package io.smallrye.reactive.messaging.aws.sqs.fault;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsFailureHandler;
import io.smallrye.reactive.messaging.aws.sqs.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;

public class SqsVisibilityFailureHandler implements SqsFailureHandler {

    @ApplicationScoped
    @Identifier(Strategy.VISIBILITY)
    public static class Factory implements SqsFailureHandler.Factory {

        @Override
        public SqsFailureHandler create(String channel, SqsAsyncClient client, Uni<String> queueUrlUni,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new SqsVisibilityFailureHandler(client, queueUrlUni);
        }
    }

    private final SqsAsyncClient client;
    private final Uni<String> queueUrlUni;

    public SqsVisibilityFailureHandler(SqsAsyncClient client, Uni<String> queueUrlUni) {
        this.client = client;
        this.queueUrlUni = queueUrlUni;
    }

    @Override
    public Uni<Void> handle(SqsMessage<?> message, Metadata metadata, Throwable throwable) {
        int timeout = metadata.get(VisibilityTimeoutMetadata.class)
                .map(VisibilityTimeoutMetadata::getVisibilityTimeout)
                .orElse(0);
        return queueUrlUni.map(queueUrl -> ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.getMessage().receiptHandle())
                .visibilityTimeout(timeout)
                .build())
                .chain(request -> Uni.createFrom().completionStage(() -> client.changeMessageVisibility(request)))
                .replaceWithVoid()
                .emitOn(message::runOnMessageContext);
    }
}
