package io.smallrye.reactive.messaging.aws.sqs.action;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsOutgoingMessage;
import io.smallrye.reactive.messaging.aws.sqs.tracing.SqsTrace;
import io.smallrye.reactive.messaging.aws.sqs.util.Helper;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.time.Duration;

import static io.smallrye.reactive.messaging.aws.sqs.tracing.SqsInstrumenter.SQS_OUTGOING_INSTRUMENTER;

public class SendMessageAction {

    public static Uni<Void> sendMessage(
            final SqsClientHolder<SqsConnectorOutgoingConfiguration> clientHolder, final SqsOutgoingMessage<?> message) {
        String payload = Helper.serialize(message, clientHolder.getJsonMapping());

        if (clientHolder.getConfig().getTracingEnabled()) {
            SqsTrace trace = new SqsTrace()
                    .withQueue(message.getTarget().getTargetName())
                    // TODO: Cannot set messageId. It is provided in response.
                    //  The intstrumenter documentation says:
                    //  Call shouldStart(Context, Object) and do not proceed if it returns false.
                    //  Call start(Context, Object) at the beginning of a request.
                    //  Call end(Context, Object, Object, Throwable) at the end of a request.
                    //  TracingUtils violates this, because end is called immediately and not at the end of a request.
                    // .withMessageId("")
                    .withConversationId(message.getSqsMetadata().getConversationId())
                    // We do not set payload size. This would require to calculate it, which is less performant.
                    ;
            TracingUtils.traceOutgoing(SQS_OUTGOING_INSTRUMENTER, message, trace);
        }

        final SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(message.getTarget().getTargetUrl())
                .messageAttributes(null)
                .messageGroupId(null)
                .messageBody(payload)

                .messageDeduplicationId(null)
                .delaySeconds(0)
                .messageSystemAttributesWithStrings(null)
                .build();

        // TODO: logging
        Uni<Void> uni = Uni.createFrom().completionStage(clientHolder.getClient().sendMessage(request))
                .onItem().transformToUni(response -> {
                    OutgoingMessageMetadata.setResultOnMessage(message, response);
                    // TODO: log
                    return Uni.createFrom().completionStage(message.ack());
                });

        // TODO: configurable retry
        if (true) {
            uni = uni.onFailure().retry()
                    .withBackOff(Duration.ofMillis(0), Duration.ofMillis(0))
                    .atMost(3);
        }

        // TODO: micrometer? Failure and Success?

        return uni;
    }
}
