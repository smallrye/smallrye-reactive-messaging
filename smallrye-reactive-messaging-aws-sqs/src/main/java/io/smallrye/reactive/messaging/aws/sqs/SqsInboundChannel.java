package io.smallrye.reactive.messaging.aws.sqs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class SqsInboundChannel {

    private final SqsClient client;
    private final Context context;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final SqsConfig config;
    private final String queueUrl;
    private final Flow.Publisher<? extends Message<?>> stream;

    public SqsInboundChannel(Vertx vertx, SqsConfig config, String queueUrl, SqsClient client) {
        this.client = client;
        this.config = config;
        this.queueUrl = queueUrl;
        this.context = Context.newInstance(
                ((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.stream = Multi.createBy().repeating()
                .uni(() -> Uni.createFrom().completionStage(this.request()))
                .until(__ -> closed.get())
                .emitOn(context::runOnContext)
                .map(SqsMessage::new);
    }

    public Flow.Publisher<? extends Message<?>> getStream() {
        return stream;
    }

    public CompletableFuture<software.amazon.awssdk.services.sqs.model.Message> request() {
        return CompletableFuture.supplyAsync(() -> {
            var receiveRequest = ReceiveMessageRequest
                    .builder().queueUrl(this.queueUrl)
                    .waitTimeSeconds(config.getWaitTimeSeconds())
                    .maxNumberOfMessages(1)
                    .build();
            // todo: error handling
            var messages = client.receiveMessage(receiveRequest).messages();
            if (messages.isEmpty()) {
                return null;
            }
            return messages.get(0);
        });
    }

    public void close() {
        closed.compareAndSet(false, true);
        client.close();
    }
}
