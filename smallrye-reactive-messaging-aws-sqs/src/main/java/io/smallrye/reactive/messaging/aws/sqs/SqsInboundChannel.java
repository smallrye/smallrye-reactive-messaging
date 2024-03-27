package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging.log;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

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
                .completionStage(this::request)
                .until(__ -> closed.get())
                .emitOn(context::runOnContext)
                .onItem().transformToMultiAndConcatenate(messages -> {
                    if (messages == null) {
                        return Multi.createFrom().empty();
                    }
                    return Multi.createFrom().iterable(messages);
                })
                .map(SqsMessage::new);
    }

    public Flow.Publisher<? extends Message<?>> getStream() {
        return stream;
    }

    public CompletableFuture<List<software.amazon.awssdk.services.sqs.model.Message>> request() {
        return CompletableFuture.supplyAsync(() -> {
            var receiveRequest = ReceiveMessageRequest
                    .builder().queueUrl(this.queueUrl)
                    .waitTimeSeconds(config.getWaitTimeSeconds())
                    .maxNumberOfMessages(config.getMaxNumberOfMessages())
                    .build();
            try {
                var messages = client.receiveMessage(receiveRequest).messages();
                if (messages.isEmpty()) {
                    log.receivedEmptyMessage();
                    return null;
                }
                return messages.stream().map(m -> {
                    log.receivedMessage(m.body());
                    return m;
                }).collect(Collectors.toList());
            } catch (SqsException e) {
                log.errorReceivingMessage(e.getMessage());
                return null;
            }
        });
    }

    public void close() {
        closed.compareAndSet(false, true);
        client.close();
    }
}
