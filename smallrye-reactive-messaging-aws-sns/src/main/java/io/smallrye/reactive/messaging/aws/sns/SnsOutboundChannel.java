package io.smallrye.reactive.messaging.aws.sns;

import static io.smallrye.reactive.messaging.aws.sns.i18n.AwsSnsLogging.log;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchResultEntry;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class SnsOutboundChannel {

    private static final String DATA_TYPE_STRING = "String";

    private final SnsAsyncClient client;
    private final String channel;
    private final JsonMapping jsonMapping;
    private final boolean healthEnabled;
    private final Uni<String> topicArnUni;

    private final boolean batch;
    private final Duration batchDelay;
    private final int batchSize;

    private final String groupId;
    private final String messageStructure;
    private final String emailSubject;
    private final String smsPhoneNumber;

    private final Flow.Subscriber<? extends Message<?>> subscriber;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<Throwable> failures = new ArrayList<>();

    public SnsOutboundChannel(final SnsConnectorOutgoingConfiguration conf,
            final SnsManager snsManager, final JsonMapping jsonMapping) {

        this.client = snsManager.getClient(conf);
        this.channel = conf.getChannel();
        this.jsonMapping = jsonMapping;
        this.healthEnabled = conf.getHealthEnabled();
        this.topicArnUni = snsManager.getTopicArn(conf).memoize().indefinitely();

        this.batch = conf.getBatch();
        this.batchSize = conf.getBatchSize();
        this.batchDelay = Duration.ofMillis(conf.getBatchDelay());

        this.groupId = conf.getGroupId().orElse(null);
        this.messageStructure = conf.getMessageStructure().orElse(null);
        this.emailSubject = conf.getEmailSubject().orElse(null);
        this.smsPhoneNumber = conf.getSmsPhoneNumber().orElse(null);

        this.subscriber = MultiUtils.via(multi -> multi
                .onSubscription().call(s -> topicArnUni)
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
                    log.unableToDispatch(channel, f);
                    reportFailure(f);
                }));
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return subscriber;
    }

    private Uni<Void> publishMessage(final SnsAsyncClient client, final List<Message<?>> messages) {
        if (closed.get()) {
            return Uni.createFrom().voidItem();
        }
        if (messages.isEmpty()) {
            return Uni.createFrom().nullItem();
        }
        if (messages.size() == 1) {
            return publishMessage(client, messages.get(0));
        }
        return topicArnUni.map(topicArn -> getPublishRequest(topicArn, messages))
                .chain(request -> Uni.createFrom().completionStage(() -> client.publishBatch(request)))
                .onItem().transformToUni(response -> {
                    var results = new ArrayList<Uni<Void>>();
                    for (BatchResultErrorEntry entry : response.failed()) {
                        var index = Integer.parseInt(entry.id());
                        if (messages.size() > index) {
                            var m = messages.get(index);
                            var failure = new BatchResultErrorException(entry);
                            results.add(Uni.createFrom().completionStage(m.nack(failure)));
                            log.unableToDispatch(channel, failure);
                        }
                    }
                    for (PublishBatchResultEntry entry : response.successful()) {
                        var index = Integer.parseInt(entry.id());
                        if (messages.size() > index) {
                            var m = messages.get(index);

                            var r = PublishResponse.builder()
                                    .messageId(entry.messageId())
                                    .sequenceNumber(entry.sequenceNumber())
                                    .build();

                            log.messageSentToChannel(channel, r.messageId(), r.sequenceNumber());
                            OutgoingMessageMetadata.setResultOnMessage(m, r);
                            results.add(Uni.createFrom().completionStage(m.ack()));
                        }
                    }
                    return Uni.combine().all().unis(results).discardItems();
                })
                .onFailure().recoverWithUni(t -> {
                    var results = new ArrayList<Uni<Void>>();
                    for (Message<?> m : messages) {
                        results.add(Uni.createFrom().completionStage(m.nack(t)));
                    }
                    log.unableToDispatch(channel, t);
                    reportFailure(t);
                    return Uni.combine().all().unis(results).discardItems();
                });
    }

    private PublishBatchRequest getPublishRequest(final String topicArn, final List<Message<?>> messages) {
        final var entries = getPublishBatchRequestEntries(topicArn, messages);
        return PublishBatchRequest.builder()
                .publishBatchRequestEntries(entries)
                .topicArn(topicArn)
                .build();
    }

    private List<PublishBatchRequestEntry> getPublishBatchRequestEntries(final String topicArn,
            final List<Message<?>> messages) {
        return IntStream.range(0, messages.size())
                .mapToObj(i -> publishBatchRequestEntry(topicArn, String.valueOf(i), messages.get(i)))
                .collect(Collectors.toList());
    }

    private PublishBatchRequestEntry publishBatchRequestEntry(final String topicArn, final String id,
            final Message<?> message) {
        final var request = getPublishRequest(topicArn, message);
        return PublishBatchRequestEntry.builder()
                .id(id)
                .messageAttributes(request.messageAttributes())
                .messageGroupId(request.messageGroupId())
                .messageDeduplicationId(request.messageDeduplicationId())
                .messageStructure(request.messageStructure())
                .subject(request.subject())
                .message(request.message())
                .build();
    }

    private Uni<Void> publishMessage(final SnsAsyncClient client, final Message<?> m) {
        if (closed.get()) {
            return Uni.createFrom().voidItem();
        }
        if (m.getPayload() == null) {
            return Uni.createFrom().nullItem();
        }
        return topicArnUni.map(topicArn -> getPublishRequest(topicArn, m))
                .chain(request -> Uni.createFrom().completionStage(() -> client.publish(request)))
                .invoke(r -> log.messageSentToChannel(channel, r.messageId(), r.sequenceNumber()))
                .onFailure().invoke(failure -> {
                    log.unableToDispatch(channel, failure);
                    reportFailure(failure);
                })
                .onItemOrFailure().transformToUni((response, t) -> {
                    if (t == null) {
                        OutgoingMessageMetadata.setResultOnMessage(m, response);
                        return Uni.createFrom().completionStage(m.ack());
                    } else {
                        return Uni.createFrom().completionStage(m.nack(t));
                    }
                });
    }

    private PublishRequest getPublishRequest(final String channelTopicArn, final Message<?> message) {
        final var payload = message.getPayload();
        var topicArn = channelTopicArn;
        if (payload instanceof final PublishRequest publishRequest) {
            return publishRequest;
        }

        if (payload instanceof final PublishRequest.Builder builder) {
            builder.topicArn(topicArn);
            if (groupId != null) {
                builder.messageGroupId(groupId);
            }
            return builder.build();
        }

        final var builder = PublishRequest.builder();
        final var messageAttributes = new HashMap<String, MessageAttributeValue>();
        var groupId = this.groupId;
        var messageStructure = this.messageStructure;
        var emailSubject = this.emailSubject;
        var smsPhoneNumber = this.smsPhoneNumber;
        final var metadata = message.getMetadata(SnsOutboundMetadata.class);

        if (metadata.isPresent()) {
            final var md = metadata.get();
            if (md.getTopicArn() != null) {
                topicArn = md.getTopicArn();
            }
            if (md.getDeduplicationId() != null) {
                builder.messageDeduplicationId(md.getDeduplicationId());
            }
            if (md.getGroupId() != null) {
                groupId = md.getGroupId();
            }
            if (md.getMessageAttributes() != null) {
                messageAttributes.putAll(md.getMessageAttributes());
            }
            if (md.getMessageStructure() != null) {
                messageStructure = md.getMessageStructure();
            }
            if (md.getEmailSubject() != null) {
                emailSubject = md.getEmailSubject();
            }
            if (md.getSmsPhoneNumber() != null) {
                smsPhoneNumber = md.getSmsPhoneNumber();
            }
        }

        final var serializedMsg = getSerializedOutgoingMessage(payload, messageAttributes);

        return builder
                .topicArn(topicArn)
                .messageGroupId(groupId)
                .messageAttributes(messageAttributes)
                .message(serializedMsg)
                .messageStructure(messageStructure)
                .subject(emailSubject)
                .phoneNumber(smsPhoneNumber)
                .build();
    }

    private String getSerializedOutgoingMessage(final Object payload,
            final Map<String, MessageAttributeValue> messageAttributes) {
        if (payload instanceof String || payload.getClass().isPrimitive() || isPrimitiveBoxed(payload.getClass())) {
            messageAttributes.put(SnsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                    .dataType(DATA_TYPE_STRING)
                    .stringValue(payload.getClass().getName())
                    .build());
            return String.valueOf(payload);
        } else if (payload.getClass().isArray() && payload.getClass().getComponentType().equals(Byte.TYPE)) {
            return new String((byte[]) payload, StandardCharsets.UTF_8);
        } else if (jsonMapping != null) {
            messageAttributes.put(SnsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                    .dataType(DATA_TYPE_STRING)
                    .stringValue(payload.getClass().getName())
                    .build());
            return jsonMapping.toJson(payload);
        }
        return String.valueOf(payload);
    }

    private boolean isPrimitiveBoxed(final Class<?> c) {
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

    private synchronized void reportFailure(final Throwable failure) {
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);
    }

    public void isAlive(final HealthReport.HealthReportBuilder builder) {
        if (healthEnabled) {
            final List<Throwable> actualFailures;
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
