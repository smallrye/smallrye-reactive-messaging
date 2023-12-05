package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.config.ConfigHelper.parseToList;
import static io.smallrye.reactive.messaging.aws.sqs.action.ReceiveMessageAction.receiveMessages;
import static io.smallrye.reactive.messaging.aws.sqs.i18n.SqsLogging.log;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsIncomingMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextOperator;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.impl.VertxInternal;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class SqsIncomingChannel extends SqsChannel {

    // TODO: remove if not needed.
    private final EventLoopContext context;

    private final SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder;

    private final Flow.Publisher<? extends Message<?>> publisher;

    private final AtomicInteger counter = new AtomicInteger(0);

    public SqsIncomingChannel(SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder) {
        this.clientHolder = clientHolder;
        this.context = ((VertxInternal) clientHolder.getVertx().getDelegate()).createEventLoopContext();

        // For performance reasons created outside the stream.
        final Supplier<Uni<ReceiveMessageResponse>> receiveMessagesSupplier = receiveMessagesSupplier();

        this.publisher = Multi.createBy().repeating()
                .uni(receiveMessagesSupplier::get)
                .until(ignore -> isClosed() && counter.get() == 0)
                .onItem().invoke(counter::decrementAndGet)
                .skip().where(response -> !response.hasMessages())
                .onItem().transformToMultiAndConcatenate(this::createMultiOfMessages)
                .onItem().transformToUniAndConcatenate(this::initCallbacks)

                // TODO: I think we do not need it in case the AWS SDK client is running on the netty eventloop thread.
                //                .emitOn(command -> context.runOnContext(event -> command.run()))

                // TODO: I think we do not need it in case the AWS SDK client is running on the netty eventloop thread.
                //                .emitOn(context.nettyEventLoop())
                .plug(ContextOperator::apply);
    }

    private Supplier<Uni<ReceiveMessageResponse>> receiveMessagesSupplier() {
        // TODO: is there an easier way to get list and map configuration? Otherwise, it is important to make
        //  sure that the data is not parsed too often. :(
        final List<String> attributeNames = parseToList(clientHolder.getConfig().getAttributeNames());
        final List<String> messageAttributeNames = parseToList(clientHolder.getConfig().getMessageAttributeNames());

        return () -> clientHolder.getTargetResolver().resolveTarget(clientHolder)
                .onItem()
                .transformToUni(target -> {
                    if (isClosed()) {
                        return Uni.createFrom().item(ReceiveMessageResponse.builder().build());
                    }
                    counter.incrementAndGet();
                    return receiveMessages(clientHolder, target, attributeNames, messageAttributeNames);
                });
    }

    private Multi<SqsIncomingMessage<?>> createMultiOfMessages(ReceiveMessageResponse response) {
        // TODO: This is not good enough. We should wait for processing of messages.
        //  also check the stream with until condition. Does this skip the last messages? Or is it checked after
        //  processing? I think the later.
        return Multi.createFrom().iterable(response.messages())
                .onItem().transform(SqsIncomingMessage::from);
    }

    private Uni<SqsIncomingMessage<?>> initCallbacks(SqsIncomingMessage<?> msg) {
        return Uni.createFrom().item(msg);
    }

    public void close() {
        // TODO: close gracefully
        super.close();
        // TODO: What can we do here? Check again the Quarkus impl regarding graceful shutdown and how this worked
        //  and blocked the shutdown.
        while (counter.get() != 0) {
            log.shutdownProgress(
                    counter.get(),
                    clientHolder.getConfig().getQueue().orElse(clientHolder.getConfig().getChannel())
            );
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

                throw new RuntimeException(e);
            }
        }
    }

    public Flow.Publisher<? extends Message<?>> getPublisher() {
        return publisher;
    }
}
