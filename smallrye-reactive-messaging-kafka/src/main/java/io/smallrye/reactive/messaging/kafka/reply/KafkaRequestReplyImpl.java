package io.smallrye.reactive.messaging.kafka.reply;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.OUTGOING_PREFIX;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.impl.ConfigHelper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.TopicPartitions;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.impl.ConnectorConfig;
import io.smallrye.reactive.messaging.providers.impl.OverrideConnectorConfig;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.vertx.mutiny.core.Vertx;

@Experimental("Experimental API")
public class KafkaRequestReplyImpl<Req, Rep> extends MutinyEmitterImpl<Req>
        implements KafkaRequestReply<Req, Rep>, MultiSubscriber<KafkaRecord<?, Rep>> {

    private final Map<CorrelationId, PendingReplyImpl<Rep>> pendingReplies = new ConcurrentHashMap<>();
    private final AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();
    private final String channel;
    private final String replyTopic;
    private final int replyPartition;
    private final Duration replyTimeout;
    private final String replyCorrelationIdHeader;
    private final String replyTopicHeader;
    private final String replyPartitionHeader;
    private final CorrelationIdHandler correlationIdHandler;
    private final ReplyFailureHandler replyFailureHandler;
    private final String autoOffsetReset;
    private final KafkaSource<Object, Rep> replySource;
    private final Set<TopicPartition> waitForPartitions;
    private final boolean gracefulShutdown;
    private Function<Message<Rep>, Message<Rep>> replyConverter;

    public KafkaRequestReplyImpl(EmitterConfiguration config,
            long defaultBufferSize,
            Config channelConfiguration,
            Instance<Map<String, Object>> configurations,
            Vertx vertx,
            KafkaCDIEvents kafkaCDIEvents,
            Instance<KafkaCommitHandler.Factory> commitHandlerFactory,
            Instance<KafkaFailureHandler.Factory> failureHandlerFactories,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            Instance<CorrelationIdHandler> correlationIdHandlers,
            Instance<ReplyFailureHandler> replyFailureHandlers,
            Instance<KafkaConsumerRebalanceListener> rebalanceListeners) {
        super(config, defaultBufferSize);
        this.channel = config.name();
        ConnectorConfig connectorConfig = new OverrideConnectorConfig(OUTGOING_PREFIX, channelConfiguration, channel,
                "reply", Map.of(
                        "topic", c -> c.getOriginalValue("topic", String.class).orElse(channel) + DEFAULT_REPLIES_TOPIC_SUFFIX,
                        "assign-seek",
                        c -> c.getOriginalValue(REPLY_PARTITION_KEY, Integer.class).map(String::valueOf).orElse(null)));
        Config replyKafkaConfig = ConfigHelper.retrieveChannelConfiguration(configurations, connectorConfig);
        KafkaConnectorIncomingConfiguration consumerConfig = new KafkaConnectorIncomingConfiguration(replyKafkaConfig);
        this.replyTopic = consumerConfig.getTopic().orElse(null);
        this.replyPartition = connectorConfig.getOptionalValue(REPLY_PARTITION_KEY, Integer.class).orElse(-1);
        this.replyTimeout = Duration.ofMillis(connectorConfig.getOptionalValue(REPLY_TIMEOUT_KEY, Integer.class).orElse(5000));

        this.autoOffsetReset = consumerConfig.getAutoOffsetReset();
        this.replyCorrelationIdHeader = connectorConfig.getOptionalValue(REPLY_CORRELATION_ID_HEADER_KEY, String.class)
                .orElse(DEFAULT_REPLY_CORRELATION_ID_HEADER);
        this.replyTopicHeader = connectorConfig.getOptionalValue(REPLY_TOPIC_HEADER_KEY, String.class)
                .orElse(DEFAULT_REPLY_TOPIC_HEADER);
        this.replyPartitionHeader = connectorConfig.getOptionalValue(REPLY_PARTITION_HEADER_KEY, String.class)
                .orElse(DEFAULT_REPLY_PARTITION_HEADER);
        String correlationIdHandlerIdentifier = connectorConfig.getOptionalValue(REPLY_CORRELATION_ID_HANDLER_KEY, String.class)
                .orElse(DEFAULT_CORRELATION_ID_HANDLER);
        this.correlationIdHandler = CDIUtils.getInstanceById(correlationIdHandlers, correlationIdHandlerIdentifier).get();
        this.replyFailureHandler = connectorConfig.getOptionalValue(REPLY_FAILURE_HANDLER_KEY, String.class)
                .map(id -> CDIUtils.getInstanceById(replyFailureHandlers, id, () -> null))
                .orElse(null);

        String consumerGroup = consumerConfig.getGroupId().orElseGet(() -> UUID.randomUUID().toString());
        this.waitForPartitions = getWaitForPartitions(consumerConfig);
        this.gracefulShutdown = consumerConfig.getGracefulShutdown();
        this.replySource = new KafkaSource<>(vertx, consumerGroup, consumerConfig,
                commitHandlerFactory, failureHandlerFactories, rebalanceListeners, kafkaCDIEvents,
                deserializationFailureHandlers, -1);

        if (consumerConfig.getBatch()) {
            replySource.getBatchStream()
                    .call(record -> Uni.createFrom().completionStage(record::ack))
                    .flatMap(record -> Multi.createFrom().iterable(record.getRecords()))
                    .subscribe(this);
        } else {
            replySource.getStream()
                    .call(record -> Uni.createFrom().completionStage(record::ack))
                    .subscribe(this);
        }
    }

    private Set<TopicPartition> getWaitForPartitions(KafkaConnectorIncomingConfiguration consumerConfig) {
        Set<String> topics = KafkaSource.getTopics(consumerConfig);
        String seekToOffset = consumerConfig.getAssignSeek().orElse(null);
        Map<TopicPartition, Optional<Long>> offsetSeeks = KafkaSource.getOffsetSeeks(seekToOffset, channel, topics);
        if (offsetSeeks.isEmpty()) {
            return topics.stream().map(t -> TopicPartitions.getTopicPartition(t, -1)).collect(Collectors.toSet());
        } else {
            return offsetSeeks.keySet();
        }
    }

    @Override
    public Flow.Publisher<Message<? extends Req>> getPublisher() {
        return this.publisher
                .plug(m -> "latest".equals(autoOffsetReset)
                        ? m.onSubscription().call(() -> waitForAssignments().ifNoItem().after(replyTimeout).fail())
                        : m)
                .onTermination().invoke(this::complete);
    }

    @Override
    public void complete() {
        super.complete();
        Subscriptions.cancel(subscription);
        if (gracefulShutdown) {
            int waitIteration = 0;
            while (!pendingReplies.isEmpty() && waitIteration < 10) {
                grace(replyTimeout.dividedBy(10));
                waitIteration++;
            }
            if (!pendingReplies.isEmpty()) {
                log.warnf("There are still %d pending replies after the closing timeout: %s",
                        pendingReplies.size(), pendingReplies.keySet());
            }
        }
        replySource.closeQuietly();
    }

    private void grace(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Uni<Rep> request(Req request) {
        return request(ContextAwareMessage.of(request))
                .map(Message::getPayload);
    }

    @Override
    public Uni<Message<Rep>> request(Message<Req> request) {
        var builder = request.getMetadata(OutgoingKafkaRecordMetadata.class)
                .map(metadata -> OutgoingKafkaRecordMetadata.from(metadata))
                .orElseGet(OutgoingKafkaRecordMetadata::builder);
        CorrelationId correlationId = correlationIdHandler.generate(request);
        builder.addHeaders(new RecordHeader(replyCorrelationIdHeader, correlationId.toBytes()),
                new RecordHeader(replyTopicHeader, replyTopic.getBytes()));
        if (replyPartition != -1) {
            byte[] partition = KafkaRequestReply.replyPartitionToBytes(replyPartition);
            builder.addHeaders(new RecordHeader(replyPartitionHeader, partition));
        }
        OutgoingMessageMetadata<RecordMetadata> outMetadata = new OutgoingMessageMetadata<>();
        return sendMessage(request.addMetadata(builder.build()).addMetadata(outMetadata))
                .invoke(() -> subscription.get().request(1))
                .chain(unused -> Uni.createFrom().<Message<Rep>> emitter(emitter -> pendingReplies.put(correlationId,
                        new PendingReplyImpl<>(outMetadata.getResult(), replyTopic, replyPartition,
                                (UniEmitter<Message<Rep>>) emitter)))
                        .ifNoItem().after(replyTimeout).fail())
                .onItemOrFailure().invoke(() -> pendingReplies.remove(correlationId))
                .plug(uni -> replyFailureHandler != null ? uni.onItem().transformToUni(f -> {
                    Throwable failure = replyFailureHandler.handleReply((KafkaRecord<?, ?>) f);
                    return failure != null ? Uni.createFrom().failure(failure) : Uni.createFrom().item(f);
                }) : uni)
                .plug(uni -> replyConverter != null ? uni.map(f -> replyConverter.apply(f)) : uni);
    }

    @Override
    public Uni<Set<TopicPartition>> waitForAssignments() {
        return replySource.getConsumer().runOnPollingThread(c -> {
            return waitForPartitions.stream()
                    .flatMap(tp -> (tp.partition() == -1) ? c.partitionsFor(tp.topic()).stream()
                            .map(pi -> TopicPartitions.getTopicPartition(tp.topic(), pi.partition()))
                            : Stream.of(tp))
                    .collect(Collectors.toSet());
        }).chain(waitFor -> waitForAssignments(waitFor));
    }

    @Override
    public Uni<Set<TopicPartition>> waitForAssignments(Collection<TopicPartition> topicPartitions) {
        return replySource.getConsumer().getAssignments()
                .repeat().whilst(tp -> !tp.containsAll(topicPartitions))
                .skip().where(Set::isEmpty)
                .toUni();
    }

    void setReplyConverter(Function<Message<Rep>, Message<Rep>> converterFunction) {
        this.replyConverter = converterFunction;
    }

    @Override
    public Map<CorrelationId, PendingReply> getPendingReplies() {
        return new HashMap<>(pendingReplies);
    }

    @Override
    public KafkaConsumer<?, Rep> getConsumer() {
        return replySource.getConsumer();
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (Subscriptions.setIfEmpty(this.subscription, subscription)) {
            subscription.request(1);
        }
    }

    @Override
    public void onItem(KafkaRecord<?, Rep> record) {
        Header header = record.getHeaders().lastHeader(replyCorrelationIdHeader);
        if (header != null) {
            CorrelationId correlationId = correlationIdHandler.parse(header.value());
            PendingReplyImpl<Rep> reply = pendingReplies.remove(correlationId);
            if (reply != null) {
                reply.getEmitter().complete(record);
                return;
            } else {
                log.requestReplyRecordIgnored(channel, record.getTopic(), correlationId.toString());
            }
        }
        // request more
        subscription.get().request(1);
    }

    @Override
    public void onFailure(Throwable failure) {
        log.requestReplyConsumerFailure(channel, replyTopic, failure);
    }

    @Override
    public void onCompletion() {

    }

    public static class PendingReplyImpl<Rep> implements PendingReply {

        private final RecordMetadata metadata;
        private final String replyTopic;
        private final int replyPartition;
        private final UniEmitter<Message<Rep>> emitter;

        public PendingReplyImpl(RecordMetadata metadata, String replyTopic, int replyPartition,
                UniEmitter<Message<Rep>> emitter) {
            this.replyTopic = replyTopic;
            this.replyPartition = replyPartition;
            this.metadata = metadata;
            this.emitter = emitter;
        }

        @Override
        public String replyTopic() {
            return replyTopic;
        }

        @Override
        public int replyPartition() {
            return replyPartition;
        }

        @Override
        public RecordMetadata recordMetadata() {
            return metadata;
        }

        public UniEmitter<Message<Rep>> getEmitter() {
            return emitter;
        }

        @Override
        public String toString() {
            return "PendingReply{" +
                    "metadata=" + metadata +
                    ", replyTopic='" + replyTopic + '\'' +
                    ", replyPartition=" + replyPartition +
                    '}';
        }
    }

}
