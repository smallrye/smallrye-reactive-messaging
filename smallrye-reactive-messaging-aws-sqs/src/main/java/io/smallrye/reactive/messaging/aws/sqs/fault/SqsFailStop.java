package io.smallrye.reactive.messaging.aws.sqs.fault;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging.log;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsFailureHandler;
import io.smallrye.reactive.messaging.aws.sqs.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class SqsFailStop implements SqsFailureHandler {

    @ApplicationScoped
    @Identifier(Strategy.FAIL)
    public static class Factory implements SqsFailureHandler.Factory {

        @Override
        public SqsFailureHandler create(String channel, SqsAsyncClient client, Uni<String> queueUrlUni,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new SqsFailStop(channel, reportFailure);
        }

    }

    private final String channel;
    private final BiConsumer<Throwable, Boolean> reportFailure;

    public SqsFailStop(String channel, BiConsumer<Throwable, Boolean> reportFailure) {
        this.channel = channel;
        this.reportFailure = reportFailure;
    }

    @Override
    public Uni<Void> handle(SqsMessage<?> message, Metadata metadata, Throwable throwable) {
        // We just fail and stop the client.
        log.messageNackedFailStop(channel);
        // report failure to the connector for health check
        reportFailure.accept(throwable, true);
        return Uni.createFrom().<Void> failure(throwable)
                .emitOn(message::runOnMessageContext);
    }

}
