package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.config.ConfigHelper.parseToList;
import static io.smallrye.reactive.messaging.aws.sqs.action.ReceiveMessageAction.receiveMessages;

import java.util.List;
import java.util.concurrent.Flow;
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

    private final Flow.Publisher<? extends Message<?>> publisher;

    public SqsIncomingChannel(SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder) {
        this.context = ((VertxInternal) clientHolder.getVertx().getDelegate()).createEventLoopContext();

        // For performance reasons created outside the stream.
        final Supplier<Uni<ReceiveMessageResponse>> receiveMessagesSupplier = receiveMessagesSupplier(clientHolder);

        this.publisher = Multi.createBy().repeating()
                .uni(receiveMessagesSupplier::get)
                .until(ignore -> isClosed())
                .skip().where(response -> !response.hasMessages())
                .onItem().transformToMulti(SqsIncomingChannel::createMultiOfMessages).merge()
                .onItem().transformToUniAndConcatenate(this::initCallbacks)

                // TODO: I think we do not need it in case the AWS SDK client is running on the netty eventloop thread.
                //                .emitOn(command -> context.runOnContext(event -> command.run()))

                // TODO: I think we do not need it in case the AWS SDK client is running on the netty eventloop thread.
                //                .emitOn(context.nettyEventLoop())
                .plug(ContextOperator::apply);
    }

    private static Supplier<Uni<ReceiveMessageResponse>> receiveMessagesSupplier(
            SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder) {
        // TODO: is there an easier way to get list and map configuration? Otherwise, it is important to make
        //  sure that the data is not parsed too often. :(
        final List<String> attributeNames = parseToList(clientHolder.getConfig().getAttributeNames());
        final List<String> messageAttributeNames = parseToList(clientHolder.getConfig().getMessageAttributeNames());

        return () -> clientHolder.getTargetResolver().resolveTarget(clientHolder)
                .onItem()
                .transformToUni(target -> receiveMessages(clientHolder, target, attributeNames, messageAttributeNames));
    }

    private static Multi<SqsIncomingMessage> createMultiOfMessages(ReceiveMessageResponse response) {
        return Multi.createFrom().iterable(response.messages())
                .onItem().transform(SqsIncomingMessage::from);
    }

    private Uni<SqsIncomingMessage> initCallbacks(SqsIncomingMessage msg) {
        return Uni.createFrom().item(msg);
    }

    public void close() {
        // TODO: close
    }

    public Flow.Publisher<? extends Message<?>> getPublisher() {
        return publisher;
    }
}
