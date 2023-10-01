package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.action.ReceiveMessageAction.receiveMessages;

import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.providers.locals.ContextOperator;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.impl.VertxInternal;

/**
 * @author Christopher Holomek
 * @since 01.10.2023
 */
public class SqsIncomingChannel extends SqsChannel {

    private final EventLoopContext context;

    private final Flow.Publisher<? extends Message<?>> publisher;

    public SqsIncomingChannel(SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder) {
        this.context = ((VertxInternal) clientHolder.getVertx().getDelegate()).createEventLoopContext();

        this.publisher = Multi.createBy().repeating()
                .uni(() -> (Uni<Message<?>>) receiveMessages(clientHolder, null))
                .until(ignore -> isClosed())
                .onItem().invoke(a -> {
                })

                // TODO: I think we do not need it in case the AWS SDK client is running on the netty eventloop thread.
                //                .emitOn(command -> context.runOnContext(event -> command.run()))

                // TODO: I think we do not need it in case the AWS SDK client is running on the netty eventloop thread.
                //                .emitOn(context.nettyEventLoop())
                .plug(ContextOperator::apply);
    }

    public void close() {

    }

    public Flow.Publisher<? extends Message<?>> getPublisher() {
        return publisher;
    }
}
