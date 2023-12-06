package io.smallrye.reactive.messaging.aws.sqs.ack;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.action.DeleteMessageAction;
import io.smallrye.reactive.messaging.aws.sqs.action.DeleteMessageBatchAction;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsIncomingMessage;

public class SqsAckHandler {

    private final SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder;

    private MultiEmitter<? super SqsIncomingMessage<?>> emitter;

    public SqsAckHandler(final SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder) {
        this.clientHolder = clientHolder;

        if (Boolean.TRUE.equals(clientHolder.getConfig().getDeleteBatchEnabled())) {
            Multi.createFrom().<SqsIncomingMessage<?>> emitter(e -> emitter = e)
                    .group().intoLists().of(getMaxSize(clientHolder), Duration.ofSeconds(getMaxDelay(clientHolder)))
                    .onItem().call(messages -> DeleteMessageBatchAction.deleteMessages(clientHolder, messages))
                    .subscribe().with(ignored -> {
                    });
        }
    }

    private static Integer getMaxSize(
            final SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder) {
        return Math.max(1, Math.min(clientHolder.getConfig().getDeleteBatchMaxSize(), 10));
    }

    private long getMaxDelay(final SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder) {
        return Math.max(1, Math.min(
                clientHolder.getConfig().getDeleteBatchMaxDelay(), clientHolder.getConfig().getVisibilityTimeout() - 1));
    }

    public CompletionStage<Void> handle(final SqsIncomingMessage<?> msg) {

        if (Boolean.TRUE.equals(clientHolder.getConfig().getDeleteBatchEnabled())) {
            emitter.emit(msg);
            return CompletableFuture.completedFuture(null);
        } else {
            return DeleteMessageAction.deleteMessage(clientHolder, msg).subscribeAsCompletionStage();
        }
    }
}
