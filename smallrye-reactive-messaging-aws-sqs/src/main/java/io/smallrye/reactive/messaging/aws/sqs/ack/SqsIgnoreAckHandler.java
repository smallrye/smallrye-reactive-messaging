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

public class SqsIgnoreAckHandler implements SqsAckHandler {

    @ApplicationScoped
    @Identifier(Strategy.IGNORE)
    public static class Factory implements SqsAckHandler.Factory {

        @Override
        public SqsAckHandler create(SqsConnectorIncomingConfiguration conf,
                Vertx vertx,
                SqsAsyncClient client,
                Uni<String> queueUrlUni,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new SqsIgnoreAckHandler();
        }
    }

    @Override
    public Uni<Void> handle(SqsMessage<?> message) {
        return Uni.createFrom().voidItem()
                .emitOn(message::runOnMessageContext);
    }
}
