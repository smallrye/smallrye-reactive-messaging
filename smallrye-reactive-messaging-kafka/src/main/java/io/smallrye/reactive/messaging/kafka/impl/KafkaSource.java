package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_DLQ;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_REASON;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static io.smallrye.reactive.messaging.kafka.impl.RebalanceListeners.findMatchingListener;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Header;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.commit.ContextHolder;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.health.KafkaSourceHealth;
import io.smallrye.reactive.messaging.kafka.tracing.KafkaOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.kafka.tracing.KafkaTrace;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;

public class KafkaSource<K, V> {
    private final Multi<IncomingKafkaRecord<K, V>> stream;
    private final Multi<IncomingKafkaRecordBatch<K, V>> batchStream;
    private final KafkaFailureHandler failureHandler;
    private final KafkaCommitHandler commitHandler;
    private final KafkaConnectorIncomingConfiguration configuration;
    private final List<Throwable> failures = new ArrayList<>();
    private final Set<String> topics;
    private final boolean isTracingEnabled;
    private final boolean isHealthEnabled;
    private final boolean isHealthReadinessEnabled;
    private final boolean isCloudEventEnabled;
    private final String channel;
    private volatile boolean subscribed;
    private final KafkaSourceHealth health;

    private final String group;
    private final Instance<KafkaCommitHandler.Factory> commitHandlerFactory;
    private final Instance<KafkaFailureHandler.Factory> failureHandlerFactories;
    private final int index;
    private final Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers;
    private final Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners;
    private final ReactiveKafkaConsumer<K, V> client;

    /**
     * This field stores the event loop context.
     * Using {@code ContextInternal} to distinguish it from the {@code Context} used by the user.
     */
    private final ContextInternal context;

    private final KafkaOpenTelemetryInstrumenter kafkaInstrumenter;

    public KafkaSource(Vertx vertx,
            String consumerGroup,
            KafkaConnectorIncomingConfiguration config,
            Instance<OpenTelemetry> openTelemetryInstance,
            Instance<KafkaCommitHandler.Factory> commitHandlerFactories,
            Instance<KafkaFailureHandler.Factory> failureHandlerFactories,
            Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners,
            KafkaCDIEvents kafkaCDIEvents,
            Instance<ClientCustomizer<Map<String, Object>>> configCustomizers,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            int index) {

        this.group = consumerGroup;
        this.commitHandlerFactory = commitHandlerFactories;
        this.failureHandlerFactories = failureHandlerFactories;
        this.index = index;
        this.deserializationFailureHandlers = deserializationFailureHandlers;
        this.consumerRebalanceListeners = consumerRebalanceListeners;
        this.topics = getTopics(config);
        String seekToOffset = config.getAssignSeek().orElse(null);
        Map<TopicPartition, Optional<Long>> offsetSeeks = getOffsetSeeks(seekToOffset, config.getChannel(), topics);

        Pattern pattern;
        if (config.getPattern()) {
            pattern = Pattern.compile(config.getTopic()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid Kafka incoming configuration for channel `"
                            + config.getChannel() + "`, `pattern` must be used with the `topic` attribute")));
            log.configuredPattern(config.getChannel(), pattern.toString());
        } else {
            log.configuredTopics(config.getChannel(), topics);
            pattern = null;
        }

        configuration = config;
        // We cannot use vertx.getOrCreate context as it would retrieve the same one everytime.
        // It associates the context with the caller thread which will always be the same.
        // So, we force the creation of different event loop context.
        context = ((VertxInternal) vertx.getDelegate()).createEventLoopContext();
        // fire consumer event (e.g. bind metrics)
        client = new ReactiveKafkaConsumer<>(config, configCustomizers, deserializationFailureHandlers, consumerGroup,
                index, this::reportFailure, getContext().getDelegate(), c -> kafkaCDIEvents.consumer().fire(c));

        String commitStrategy = config
                .getCommitStrategy()
                .orElse(Boolean.parseBoolean(client.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                        ? KafkaCommitHandler.Strategy.IGNORE
                        : KafkaCommitHandler.Strategy.THROTTLED);

        commitHandler = createCommitHandler(vertx, commitStrategy);
        failureHandler = createFailureHandler(vertx);
        if (configuration.getHealthEnabled()) {
            health = new KafkaSourceHealth(this, configuration, client, topics, pattern);
        } else {
            health = null;
        }

        isTracingEnabled = this.configuration.getTracingEnabled();
        isHealthEnabled = this.configuration.getHealthEnabled();
        isHealthReadinessEnabled = this.configuration.getHealthReadinessEnabled();
        isCloudEventEnabled = this.configuration.getCloudEvents();
        channel = this.configuration.getChannel();

        if (commitHandler instanceof ContextHolder) {
            ((ContextHolder) commitHandler).capture(context);
        }
        if (failureHandler instanceof ContextHolder) {
            ((ContextHolder) failureHandler).capture(context);
        }
        this.client.setRebalanceListener(findMatchingListener(config, consumerGroup, consumerRebalanceListeners),
                commitHandler);

        if (!config.getBatch()) {
            Multi<ConsumerRecord<K, V>> multi;
            if (pattern != null) {
                multi = client.subscribe(pattern);
            } else {
                if (offsetSeeks.isEmpty()) {
                    multi = client.subscribe(topics);
                } else {
                    multi = client.assignAndSeek(offsetSeeks);
                }
            }

            multi = multi.onSubscription().invoke(() -> {
                subscribed = true;
                final String groupId = client.get(ConsumerConfig.GROUP_ID_CONFIG);
                final String clientId = client.get(ConsumerConfig.CLIENT_ID_CONFIG);
                log.connectedToKafka(clientId, config.getBootstrapServers(), groupId, topics);
            });

            multi = multi.onFailure().invoke(t -> {
                log.unableToReadRecord(topics, t);
                reportFailure(t, false);
            });

            Multi<IncomingKafkaRecord<K, V>> incomingMulti = multi.onItem().transformToUni(rec -> {
                IncomingKafkaRecord<K, V> record = new IncomingKafkaRecord<>(rec, channel, index, commitHandler,
                        failureHandler, isCloudEventEnabled, isTracingEnabled);
                if ((failureHandler instanceof KafkaDeadLetterQueue)
                        && rec.headers() != null
                        && rec.headers().lastHeader(DESERIALIZATION_FAILURE_DLQ) != null) {
                    Header reasonMsgHeader = rec.headers().lastHeader(DESERIALIZATION_FAILURE_REASON);
                    String message = reasonMsgHeader != null ? new String(reasonMsgHeader.value()) : null;
                    RecordDeserializationException reason = new RecordDeserializationException(
                            TopicPartitions.getTopicPartition(record), record.getOffset(), message, null);
                    return failureHandler.handle(record, reason, record.getMetadata())
                            .onItem().transform(ignore -> null);
                }
                return commitHandler.received(record);
            }).concatenate();

            if (failureHandler instanceof KafkaDelayedRetryTopic) {
                Multi<IncomingKafkaRecord<K, V>> retryStream = (Multi<IncomingKafkaRecord<K, V>>) ((KafkaDelayedRetryTopic) failureHandler)
                        .retryStream();
                incomingMulti = Multi.createBy().merging().withConcurrency(2).streams(incomingMulti, retryStream);
            }

            if (config.getTracingEnabled()) {
                incomingMulti = incomingMulti.onItem().invoke(record -> incomingTrace(record, false));
            }
            this.stream = incomingMulti
                    .onFailure().invoke(t -> reportFailure(t, false));
            this.batchStream = null;
        } else {
            Multi<ConsumerRecords<K, V>> multi;
            if (pattern != null) {
                multi = client.subscribeBatch(pattern);
            } else {
                if (offsetSeeks.isEmpty()) {
                    multi = client.subscribeBatch(topics);
                } else {
                    multi = client.assignAndSeekBatch(offsetSeeks);
                }
            }
            multi = multi.onSubscription().invoke(() -> {
                subscribed = true;
                final String groupId = client.get(ConsumerConfig.GROUP_ID_CONFIG);
                final String clientId = client.get(ConsumerConfig.CLIENT_ID_CONFIG);
                log.connectedToKafka(clientId, config.getBootstrapServers(), groupId, topics);
            });
            multi = multi.onFailure().invoke(t -> {
                log.unableToReadRecord(topics, t);
                reportFailure(t, false);
            });

            Multi<IncomingKafkaRecordBatch<K, V>> incomingMulti = multi.onItem().transformToUni(rec -> {
                IncomingKafkaRecordBatch<K, V> batch = new IncomingKafkaRecordBatch<>(rec, channel, index,
                        commitHandler, failureHandler, isCloudEventEnabled, isTracingEnabled);
                return receiveBatchRecord(batch);
            }).concatenate();

            if (config.getTracingEnabled()) {
                incomingMulti = incomingMulti.onItem().invoke(this::incomingTrace);
            }
            this.batchStream = incomingMulti
                    .onFailure().invoke(t -> reportFailure(t, false));
            this.stream = null;
        }

        if (isTracingEnabled) {
            kafkaInstrumenter = KafkaOpenTelemetryInstrumenter.createForSource(openTelemetryInstance);
        } else {
            kafkaInstrumenter = null;
        }
    }

    public static Set<String> getTopics(KafkaConnectorIncomingConfiguration config) {
        String list = config.getTopics().orElse(null);
        String top = config.getTopic().orElse(null);
        String channel = config.getChannel();
        boolean isPattern = config.getPattern();

        if (list != null && top != null) {
            throw ex.invalidTopics(channel, "topic");
        }

        if (list != null && isPattern) {
            throw ex.invalidTopics(channel, "pattern");
        }

        if (list != null) {
            String[] strings = list.split(",");
            return Arrays.stream(strings).map(String::trim).collect(Collectors.toSet());
        } else if (top != null) {
            return Collections.singleton(top);
        } else {
            return Collections.singleton(channel);
        }
    }

    public static Map<TopicPartition, Optional<Long>> getOffsetSeeks(String seekToOffset, String channel, Set<String> topics) {
        if (seekToOffset == null || seekToOffset.isBlank()) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, Optional<Long>> offsetSeeks = new HashMap<>();
        String[] tpOffsets = seekToOffset.split(",");
        for (String tpOffset : tpOffsets) {
            String[] tpo = tpOffset.strip().split(":");
            try {
                if (tpo.length == 3) {
                    // [topic]:[partition]:[offset]
                    String topic = tpo[0];
                    int partition = Integer.parseInt(tpo[1]);
                    long offset = Long.parseLong(tpo[2]);
                    offsetSeeks.put(TopicPartitions.getTopicPartition(topic, partition), Optional.of(offset));
                } else if (tpo.length == 2) {
                    try {
                        // [partition]:[offset]
                        int partition = Integer.parseInt(tpo[0]);
                        long offset = Long.parseLong(tpo[1]);
                        if (topics.size() > 1) {
                            throw ex.invalidAssignSeekTopic(channel, tpOffset);
                        }
                        String topic = topics.iterator().next();
                        offsetSeeks.put(TopicPartitions.getTopicPartition(topic, partition), Optional.of(offset));
                    } catch (NumberFormatException e) {
                        // [topic]:[partition]
                        String topic = tpo[0];
                        int partition = Integer.parseInt(tpo[1]);
                        offsetSeeks.put(TopicPartitions.getTopicPartition(topic, partition), Optional.empty());
                    }
                } else {
                    // [partition]
                    int partition = Integer.parseInt(tpo[0]);
                    if (topics.size() > 1) {
                        throw ex.invalidAssignSeekTopic(channel, tpOffset);
                    }
                    String topic = topics.iterator().next();
                    offsetSeeks.put(TopicPartitions.getTopicPartition(topic, partition), Optional.empty());
                }
            } catch (Throwable t) {
                throw ex.invalidAssignSeek(channel, tpOffset, t);
            }
        }
        if (offsetSeeks.keySet().stream().map(TopicPartition::topic).anyMatch(s -> !topics.contains(s))) {
            log.topicsConfigurationIgnored(topics.toString(), channel, offsetSeeks.keySet().toString());
        }
        return offsetSeeks;
    }

    public synchronized void reportFailure(Throwable failure, boolean fatal) {
        if (failure instanceof RebalanceInProgressException) {
            // Just log the failure - it will be retried
            log.failureReportedDuringRebalance(topics, failure);
            return;
        }
        log.failureReported(topics, failure);
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);

        if (fatal) {
            if (client != null) {
                client.close();
            }
        }
    }

    public void incomingTrace(IncomingKafkaRecord<K, V> kafkaRecord, boolean insideBatch) {
        if (isTracingEnabled) {
            KafkaTrace kafkaTrace = new KafkaTrace.Builder()
                    .withPartition(kafkaRecord.getPartition())
                    .withTopic(kafkaRecord.getTopic())
                    .withOffset(kafkaRecord.getOffset())
                    .withHeaders(kafkaRecord.getHeaders())
                    .withGroupId(client.get(ConsumerConfig.GROUP_ID_CONFIG))
                    .withClientId(client.get(ConsumerConfig.CLIENT_ID_CONFIG))
                    .build();

            kafkaInstrumenter.traceIncoming(kafkaRecord, kafkaTrace, !insideBatch);
        }
    }

    @SuppressWarnings("unchecked")
    public void incomingTrace(IncomingKafkaRecordBatch<K, V> kafkaBatchRecord) {
        if (isTracingEnabled) {
            for (KafkaRecord<K, V> record : kafkaBatchRecord.getRecords()) {
                IncomingKafkaRecord<K, V> kafkaRecord = record.unwrap(IncomingKafkaRecord.class);
                incomingTrace(kafkaRecord, true);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Uni<IncomingKafkaRecordBatch<K, V>> receiveBatchRecord(IncomingKafkaRecordBatch<K, V> batch) {
        List<Uni<IncomingKafkaRecord<K, V>>> records = new ArrayList<>();
        for (KafkaRecord<K, V> record : batch.getLatestOffsetRecords().values()) {
            IncomingKafkaRecord<K, V> kafkaRecord = record.unwrap(IncomingKafkaRecord.class);
            records.add(commitHandler.received(kafkaRecord));
        }
        if (records.size() == 0) {
            return Uni.createFrom().item(batch);
        }
        if (records.size() == 1) {
            return records.get(0).onItem().transform(ignored -> batch);
        }
        return Uni.combine().all().unis(records).with(ignored -> batch);
    }

    private KafkaFailureHandler createFailureHandler(Vertx vertx) {
        String strategy = configuration.getFailureStrategy();
        Instance<KafkaFailureHandler.Factory> failureHandlerFactory = failureHandlerFactories
                .select(Identifier.Literal.of(strategy));
        if (failureHandlerFactory.isResolvable()) {
            return failureHandlerFactory.get().create(configuration, vertx, client, this::reportFailure);
        } else {
            throw ex.illegalArgumentInvalidFailureStrategy(strategy);
        }

    }

    private KafkaCommitHandler createCommitHandler(Vertx vertx, String strategy) {
        Instance<KafkaCommitHandler.Factory> possibleCommitHandler = commitHandlerFactory
                .select(Identifier.Literal.of(strategy));
        if (possibleCommitHandler.isResolvable()) {
            log.commitStrategyForChannel(strategy, configuration.getChannel());
            return possibleCommitHandler.get().create(configuration, vertx, client, this::reportFailure);
        } else {
            throw ex.illegalArgumentInvalidCommitStrategy(strategy);
        }
    }

    public Multi<IncomingKafkaRecord<K, V>> getStream() {
        return stream;
    }

    public Multi<IncomingKafkaRecordBatch<K, V>> getBatchStream() {
        return batchStream;
    }

    public void closeQuietly() {
        try {
            if (configuration.getGracefulShutdown()) {
                Duration pollTimeoutTwice = Duration.ofMillis(configuration.getPollTimeout() * 2L);
                if (!this.client.isClosed() && this.client.runOnPollingThread(c -> {
                    Set<TopicPartition> partitions = c.assignment();
                    if (!partitions.isEmpty()) {
                        log.pauseAllPartitionOnTermination();
                        c.pause(partitions);
                        return true;
                    }
                    return false;
                })
                        .await().atMost(pollTimeoutTwice)) {
                    // 2 times the poll timeout - so we are sure that the last (non-empty) poll has completed.
                    grace(pollTimeoutTwice);
                }

                // If we don't have assignment, no need to wait.
            }

            this.commitHandler.terminate(configuration.getGracefulShutdown());
            this.failureHandler.terminate();
        } catch (Throwable e) {
            log.exceptionOnClose(e);
        }

        try {
            this.client.close();
        } catch (Throwable e) {
            log.exceptionOnClose(e);
        }

        if (health != null) {
            health.close();
        }
    }

    private void grace(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void isAlive(HealthReport.HealthReportBuilder builder) {
        if (isHealthEnabled) {
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

        // If health is disabled, do not add anything to the builder.
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (health != null && isHealthReadinessEnabled) {
            health.isReady(builder);
        }
        // If health is disabled, do not add anything to the builder.
    }

    public void isStarted(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (health != null) {
            health.isStarted(builder);
        }
        // If health is disabled, do not add anything to the builder.
    }

    /**
     * For testing purpose only
     *
     * @return get the underlying consumer.
     */
    public ReactiveKafkaConsumer<K, V> getConsumer() {
        return this.client;
    }

    String getConsumerGroup() {
        return group;
    }

    int getConsumerIndex() {
        return index;
    }

    Instance<DeserializationFailureHandler<?>> getDeserializationFailureHandlers() {
        return deserializationFailureHandlers;
    }

    Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return consumerRebalanceListeners;
    }

    public KafkaCommitHandler getCommitHandler() {
        return commitHandler;
    }

    io.vertx.mutiny.core.Context getContext() {
        return new io.vertx.mutiny.core.Context(context);
    }

    public String getChannel() {
        return channel;
    }

    public boolean hasSubscribers() {
        return subscribed;
    }
}
