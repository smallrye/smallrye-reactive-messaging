package io.smallrye.reactive.messaging.aws.sqs;

import java.util.function.BiConsumer;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public interface SqsAckHandler {

    interface Strategy {
        String DELETE = "delete";
        String IGNORE = "ignore";
    }

    interface Factory {
        SqsAckHandler create(SqsConnectorIncomingConfiguration conf, Vertx vertx, SqsAsyncClient client,
                Uni<String> queueUrlUni,
                BiConsumer<Throwable, Boolean> reportFailure);
    }

    default Uni<SqsMessage<?>> received(SqsMessage<?> message) {
        return Uni.createFrom().item(message);
    }

    Uni<Void> handle(SqsMessage<?> message);

    default void close(boolean graceful) {

    }

}
