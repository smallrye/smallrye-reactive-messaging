package io.smallrye.reactive.messaging.aws.sqs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SqsOutboundChannel {

    private final Flow.Subscriber<? extends Message<?>> subscriber;
    private final SqsAsyncClient client;
    private final String channel;
    private final Uni<String> queueUrlUni;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final JsonMapping jsonMapping;
    private final List<Throwable> failures = new ArrayList<>();
    private final boolean healthEnabled;
    private final String groupId;
    private final boolean batch;
    private final Duration batchDelay;
    private final int batchSize;

    public SqsOutboundChannel(SqsConnectorOutgoingConfiguration conf, SqsManager sqsManager, JsonMapping jsonMapping) {
        this.channel = conf.getChannel();
        this.healthEnabled = conf.getHealthEnabled();
        this.client = sqsManager.getClient(conf);
        this.batch = conf.getBatch();
        this.batchSize = conf.getBatchSize();
        this.batchDelay = Duration.ofMillis(conf.getBatchDelay());
        this.queueUrlUni = sqsManager.getQueueUrl(conf).memoize().indefinitely();
        this.groupId = conf.getGroupId().orElse(null);
        this.jsonMapping = jsonMapping;
        this.subscriber = MultiUtils.via(multi -> multi
                .onSubscription().call(s -> queueUrlUni)
                .plug(stream -> {
                    if (batch) {
                        return stream.group().intoLists().of(batchSize, batchDelay)
                                .call(l -> publishMessage(this.client, l))
                                .onItem().transformToMultiAndConcatenate(l -> Multi.createFrom().iterable(l));
                    } else {
                        return stream.call(m -> publishMessage(this.client, m));
                    }
                })
                .onFailure().invoke(f -> {
                    AwsSqsLogging.log.unableToDispatch(channel, f);
                    reportFailure(f);
                }));
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return subscriber;
    }

    private Uni<Void> publishMessage(SqsAsyncClient client, Message<?> m) {
        if (closed.get()) {
            return Uni.createFrom().voidItem();
        }
        if (m.getPayload() == null) {
            return Uni.createFrom().nullItem();
        }
        return queueUrlUni.map(queueUrl -> getSendMessageRequest(queueUrl, m))
                .chain(request -> Uni.createFrom().completionStage(() -> client.sendMessage(request)))
                .invoke(r -> AwsSqsLogging.log.messageSentToChannel(channel, r.messageId(), r.sequenceNumber()))
                .onItemOrFailure().transformToUni((response, t) -> {
                    if (t == null) {
                        OutgoingMessageMetadata.setResultOnMessage(m, response);
                        return Uni.createFrom().completionStage(m.ack());
                    } else {
                        return Uni.createFrom().completionStage(m.nack(t));
                    }
                });
    }

    private Uni<Void> publishMessage(SqsAsyncClient client, List<Message<?>> messages) {
        if (closed.get()) {
            return Uni.createFrom().voidItem();
        }
        if (messages.isEmpty()) {
            return Uni.createFrom().nullItem();
        }
        if (messages.size() == 1) {
            return publishMessage(client, messages.get(0));
        }
        return queueUrlUni.map(queueUrl -> getSendMessageRequest(queueUrl, messages))
                .chain(request -> Uni.createFrom().completionStage(() -> client.sendMessageBatch(request)))
                .onItem().transformToUni(response -> {
                    List<Uni<Void>> results = new ArrayList<>();
                    for (BatchResultErrorEntry entry : response.failed()) {
                        int index = Integer.parseInt(entry.id());
                        if (messages.size() > index) {
                            Message<?> m = messages.get(index);
                            var failure = new BatchResultErrorException(entry);
                            results.add(Uni.createFrom().completionStage(m.nack(failure)));
                            AwsSqsLogging.log.unableToDispatch(channel, failure);
                        }
                    }
                    for (SendMessageBatchResultEntry entry : response.successful()) {
                        int index = Integer.parseInt(entry.id());
                        if (messages.size() > index) {
                            Message<?> m = messages.get(index);
                            SendMessageResponse r = SendMessageResponse.builder()
                                    .messageId(entry.messageId())
                                    .sequenceNumber(entry.sequenceNumber())
                                    .md5OfMessageBody(entry.md5OfMessageBody())
                                    .md5OfMessageAttributes(entry.md5OfMessageAttributes())
                                    .md5OfMessageSystemAttributes(entry.md5OfMessageSystemAttributes())
                                    .build();
                            AwsSqsLogging.log.messageSentToChannel(channel, r.messageId(), r.sequenceNumber());
                            OutgoingMessageMetadata.setResultOnMessage(m, r);
                            results.add(Uni.createFrom().completionStage(m.ack()));
                        }
                    }
                    return Uni.combine().all().unis(results).discardItems();
                })
                .onFailure().recoverWithUni(t -> {
                    List<Uni<Void>> results = new ArrayList<>();
                    for (Message<?> m : messages) {
                        results.add(Uni.createFrom().completionStage(m.nack(t)));
                    }
                    AwsSqsLogging.log.unableToDispatch(channel, t);
                    reportFailure(t);
                    return Uni.combine().all().unis(results).discardItems();
                });
    }

    private SendMessageBatchRequest getSendMessageRequest(String channelQueueUrl, List<Message<?>> messages) {
        List<SendMessageBatchRequestEntry> entries = getSendMessageBatchEntry(channelQueueUrl, messages);
        return SendMessageBatchRequest.builder()
                .entries(entries)
                .queueUrl(channelQueueUrl)
                .build();
    }

    private List<SendMessageBatchRequestEntry> getSendMessageBatchEntry(String channelQueueUrl, List<Message<?>> messages) {
        // Use message index in the list as the id to identify the message in the batch result.
        return IntStream.range(0, messages.size())
                .mapToObj(i -> sendMessageBatchRequestEntry(channelQueueUrl, String.valueOf(i), messages.get(i)))
                .collect(Collectors.toList());
    }

    private SendMessageBatchRequestEntry sendMessageBatchRequestEntry(String channelQueueUrl, String id, Message<?> message) {
        SendMessageRequest request = getSendMessageRequest(channelQueueUrl, message);
        return SendMessageBatchRequestEntry.builder()
                .id(id)
                .delaySeconds(request.delaySeconds())
                .messageAttributes(request.messageAttributes())
                .messageGroupId(request.messageGroupId())
                .messageDeduplicationId(request.messageDeduplicationId())
                .messageSystemAttributes(request.messageSystemAttributes())
                .messageBody(request.messageBody())
                .build();
    }

    private SendMessageRequest getSendMessageRequest(String channelQueueUrl, Message<?> m) {
        Object payload = m.getPayload();
        String queueUrl = channelQueueUrl;
        if (payload instanceof SendMessageRequest) {
            return (SendMessageRequest) payload;
        }
        if (payload instanceof SendMessageRequest.Builder) {
            SendMessageRequest.Builder builder = ((SendMessageRequest.Builder) payload)
                    .queueUrl(queueUrl);
            if (groupId != null) {
                builder.messageGroupId(groupId);
            }
            return builder.build();
        }
        SendMessageRequest.Builder builder = SendMessageRequest.builder();
        Map<String, MessageAttributeValue> msgAttributes = new HashMap<>();
        String groupId = this.groupId;
        Optional<SqsOutboundMetadata> metadata = m.getMetadata(SqsOutboundMetadata.class);
        if (metadata.isPresent()) {
            SqsOutboundMetadata md = metadata.get();
            if (md.getQueueUrl() != null) {
                queueUrl = md.getQueueUrl();
            }
            if (md.getDeduplicationId() != null) {
                builder.messageDeduplicationId(md.getDeduplicationId());
            }
            if (md.getGroupId() != null) {
                groupId = md.getGroupId();
            }
            if (md.getDelaySeconds() != null) {
                builder.delaySeconds(md.getDelaySeconds());
            }
            if (md.getMessageAttributes() != null) {
                msgAttributes.putAll(md.getMessageAttributes());
            }
        }
        if (payload instanceof software.amazon.awssdk.services.sqs.model.Message) {
            var msg = (software.amazon.awssdk.services.sqs.model.Message) payload;
            if (msg.hasAttributes()) {
                msgAttributes.putAll(msg.messageAttributes());
            }
            return builder
                    .queueUrl(queueUrl)
                    .messageGroupId(groupId)
                    .messageAttributes(msgAttributes)
                    .messageBody(msg.body())
                    .build();
        }
        String messageBody = outgoingPayloadClassName(payload, msgAttributes);
        return builder
                .queueUrl(queueUrl)
                .messageGroupId(groupId)
                .messageAttributes(msgAttributes)
                .messageBody(messageBody)
                .build();
    }

    private String outgoingPayloadClassName(Object payload, Map<String, MessageAttributeValue> messageAttributes) {
        if (payload instanceof String || payload.getClass().isPrimitive() || isPrimitiveBoxed(payload.getClass())) {
            messageAttributes.put(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder().dataType("String")
                    .stringValue(payload.getClass().getName()).build());
            return String.valueOf(payload);
        } else if (payload.getClass().isArray() && payload.getClass().getComponentType().equals(Byte.TYPE)) {
            return new String((byte[]) payload);
        } else if (jsonMapping != null) {
            messageAttributes.put(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder().dataType("String")
                    .stringValue(payload.getClass().getName()).build());
            return jsonMapping.toJson(payload);
        }
        return String.valueOf(payload);
    }

    private boolean isPrimitiveBoxed(Class<?> c) {
        return c.equals(Boolean.class)
                || c.equals(Integer.class)
                || c.equals(Byte.class)
                || c.equals(Double.class)
                || c.equals(Float.class)
                || c.equals(Short.class)
                || c.equals(Character.class)
                || c.equals(Long.class);
    }

    public void close() {
        closed.set(true);
    }

    private synchronized void reportFailure(Throwable failure) {
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);
    }

    public void isAlive(HealthReport.HealthReportBuilder builder) {
        if (healthEnabled) {
            List<Throwable> actualFailures;
            synchronized (this) {
                actualFailures = new ArrayList<>(failures);
            }
            if (!actualFailures.isEmpty()) {
                builder.add(channel, false,
                        actualFailures.stream().map(Throwable::getMessage).collect(Collectors.joining()));
            } else {
                builder.add(channel, true);
            }
        }
    }
}
