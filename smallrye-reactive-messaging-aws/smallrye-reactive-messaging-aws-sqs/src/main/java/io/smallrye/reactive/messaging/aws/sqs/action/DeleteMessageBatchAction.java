package io.smallrye.reactive.messaging.aws.sqs.action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.SqsTarget;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsIncomingMessage;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;

public class DeleteMessageBatchAction {

    public static Uni<Void> deleteMessages(
            SqsClientHolder<SqsConnectorIncomingConfiguration> clientHolder,
            List<SqsIncomingMessage<?>> messages) {

        if (messages.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        Collection<DeleteMessageBatchRequestEntry> entries = new ArrayList<>(messages.size());

        AtomicReference<SqsTarget> target = new AtomicReference<>();
        messages.forEach(message -> {
            String id = UUID.randomUUID().toString();
            DeleteMessageBatchRequestEntry entry = DeleteMessageBatchRequestEntry.builder().id(id)
                    .receiptHandle(message.getSqsMetadata().getAwsMessage().receiptHandle()).build();

            target.set(message.getTarget());

            entries.add(entry);

        });

        DeleteMessageBatchRequest request = DeleteMessageBatchRequest.builder().queueUrl(target.get().getTargetUrl())
                .entries(entries).build();

        return Uni.createFrom().completionStage(clientHolder.getClient().deleteMessageBatch(request)).replaceWithVoid();
    }
}
