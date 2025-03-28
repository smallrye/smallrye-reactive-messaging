package io.smallrye.reactive.messaging.aws.sqs.ack;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsAckHandler;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.SqsMessage;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

public class SqsDeleteAckHandler implements SqsAckHandler {

    private final SqsAsyncClient client;
    private final Uni<String> queueUrlUni;

    @ApplicationScoped
    @Identifier(Strategy.DELETE)
    public static class Factory implements SqsAckHandler.Factory {

        @Override
        public SqsAckHandler create(SqsConnectorIncomingConfiguration conf, Vertx vertx,
                SqsAsyncClient client,
                Uni<String> queueUrlUni,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new SqsDeleteAckHandler(client, queueUrlUni);
        }
    }

    public SqsDeleteAckHandler(SqsAsyncClient client, Uni<String> queueUrlUni) {
        this.client = client;
        this.queueUrlUni = queueUrlUni;
    }

    @Override
    public Uni<Void> handle(SqsMessage<?> message) {
        return queueUrlUni.map(queueUrl -> DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.getMessage().receiptHandle())
                .build())
                .chain(request -> Uni.createFrom().completionStage(() -> client.deleteMessage(request)))
                .replaceWithVoid()
                .emitOn(message::runOnMessageContext);
    }
}
