package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.tracing.SqsInstrumenter.SQS_OUTGOING_INSTRUMENTER;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.aws.sqs.action.SendMessageAction;
import io.smallrye.reactive.messaging.aws.sqs.action.SendMessageBatchAction;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessage;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsOutgoingMessage;
import io.smallrye.reactive.messaging.aws.sqs.tracing.SqsTrace;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

public class SqsOutgoingChannel extends SqsChannel {

    private final SqsClientHolder<SqsConnectorOutgoingConfiguration> clientHolder;
    private final Flow.Subscriber<? extends Message<?>> subscriber;

    public SqsOutgoingChannel(SqsClientHolder<SqsConnectorOutgoingConfiguration> clientHolder) {
        this.clientHolder = clientHolder;

        if (Boolean.TRUE.equals(clientHolder.getConfig().getSendBatchEnabled())) {
            subscriber = MultiUtils.via(this::createStreamWithBatching);
        } else {
            subscriber = MultiUtils.via(this::createStream);
        }
    }

    private Multi<? extends SqsOutgoingMessage<?>> prepare(Multi<Message<?>> m) {
        return m
                .onItem().transform(SqsOutgoingMessage::from)
                .onItem().transformToUniAndConcatenate(this::addTargetInformation)
                .onItem().invoke(this::tracing);
    }

    private Multi<Void> createStream(Multi<Message<?>> m) {
        return prepare(m)
                .onItem().transformToUniAndConcatenate(this::sendMessage);
        //                .onFailure().invoke(log::unableToSend)
    }

    private Multi<Void> createStreamWithBatching(Multi<Message<?>> m) {
        // We need to group by target first and then into lists for batching.
        // Reason is that queue can be overwritten in messages. We cannot group into lists directly. Otherwise,
        // we would send messages to the wrong queue.
        return prepare(m)
                .group().by(SqsMessage::getTarget)
                .onItem().transformToMultiAndMerge(group -> group
                        .group().intoLists().of(10, Duration.ofMillis(3000))
                        .onItem().transform(msg -> Tuple2.of(group.key(), msg)))
                .onItem().transformToUniAndConcatenate(tuple -> sendBatchMessage(tuple.getItem1(), tuple.getItem2()));
    }

    private Uni<? extends SqsOutgoingMessage<?>> addTargetInformation(SqsOutgoingMessage<?> msg) {
        return clientHolder.getTargetResolver().resolveTarget(clientHolder)
                .onItem().transform(target -> {
                    msg.withTarget(target);
                    return msg;
                });
    }

    private Uni<Void> sendMessage(SqsOutgoingMessage<?> message) {
        return SendMessageAction.sendMessage(clientHolder, message);
    }

    private Uni<Void> sendBatchMessage(SqsTarget target, List<? extends SqsOutgoingMessage<?>> messages) {
        return SendMessageBatchAction.sendMessage(clientHolder, target, messages);
    }

    private void tracing(SqsOutgoingMessage<?> message) {
        if (clientHolder.getConfig().getTracingEnabled()) {
            SqsTrace trace = new SqsTrace()
                    .withQueue(message.getTarget().getTargetName())
                    // TODO: Cannot set messageId. It is provided in response.
                    //  The intstrumenter documentation says:
                    //  Call shouldStart(Context, Object) and do not proceed if it returns false.
                    //  Call start(Context, Object) at the beginning of a request.
                    //  Call end(Context, Object, Object, Throwable) at the end of a request.
                    //  TracingUtils violates this, because end is called immediately and not at the end of a request.
                    //  So it would be necessary to update the trace at response and then end the span.
                    //  This is not possible.
                    //  For batching it starts here and might be delayed extremely. This will not be visible.
                    //  Furthermore, it is an issue to set the messageIds. They are available in the action, but
                    //  we cannot wait until this happened. Updating messageId in response would be ok as well, but not
                    //  possible as mentioned. Or it would be necessary to create the ids here. I do not like that.
                    // .withMessageId("")
                    .withConversationId(message.getSqsMetadata().getConversationId())
            // We do not set payload size. This would require to calculate it, which is less performant.
            ;
            TracingUtils.traceOutgoing(SQS_OUTGOING_INSTRUMENTER, message, trace);
        }
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return subscriber;
    }

    public void close() {
        // TODO: close
    }
}
