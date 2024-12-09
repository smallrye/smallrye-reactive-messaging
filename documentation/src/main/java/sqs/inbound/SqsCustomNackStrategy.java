package sqs.inbound;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsFailureHandler;
import io.smallrye.reactive.messaging.aws.sqs.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class SqsCustomNackStrategy implements SqsFailureHandler {

    @ApplicationScoped
    @Identifier("custom")
    public static class Factory implements SqsFailureHandler.Factory {

        @Override
        public SqsFailureHandler create(String channel, SqsAsyncClient client, Uni<String> queueUrlUni,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new SqsCustomNackStrategy();
        }
    }

    @Override
    public Uni<Void> handle(SqsMessage<?> message, Metadata metadata, Throwable throwable) {
        return Uni.createFrom().voidItem()
                .emitOn(message::runOnMessageContext);
    }
}
