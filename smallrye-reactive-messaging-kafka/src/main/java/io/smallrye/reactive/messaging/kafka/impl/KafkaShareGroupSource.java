package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_REASON;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_SHARE_ACK_TYPE;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaShareGroupRecordBatch;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.commit.ContextHolder;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.commit.KafkaShareGroupCommit;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaShareGroupFailureHandler;
import io.smallrye.reactive.messaging.kafka.health.KafkaShareGroupSourceHealth;
import io.smallrye.reactive.messaging.kafka.queues.ShareGroupAcknowledgement;
import io.smallrye.reactive.messaging.kafka.tracing.KafkaOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.kafka.tracing.KafkaTrace;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;

public class KafkaShareGroupSource<K, V> {
    private final Multi<IncomingKafkaRecord<K, V>> stream;
    private final Multi<IncomingKafkaShareGroupRecordBatch<K, V>> batchStream;
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
    private final KafkaShareGroupSourceHealth health;

    private final String group;
    private final ReactiveKafkaShareConsumer<K, V> client;

    /**
     * This field stores the event loop context.
     * Using {@code ContextInternal} to distinguish it from the {@code Context} used by the user.
     */
    private final ContextInternal context;

    private final KafkaOpenTelemetryInstrumenter kafkaInstrumenter;

    public KafkaShareGroupSource(Vertx vertx,
            String consumerGroup,
            KafkaConnectorIncomingConfiguration config,
            Instance<OpenTelemetry> openTelemetryInstance,
            KafkaCDIEvents kafkaCDIEvents,
            KafkaAdminClientRegistry adminClientRegistry,
            Instance<ClientCustomizer<Map<String, Object>>> configCustomizers,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers) {

        this.group = consumerGroup;
        this.topics = getTopics(config);

        // Log incompatible configuration for share groups
        List<String> incompatibleConfig = new ArrayList<>();
        if (config.getPattern()) {
            incompatibleConfig.add("pattern");
        }
        if (config.getAssignSeek().isPresent()) {
            incompatibleConfig.add("assign-seek");
        }
        if (config.getPartitions() > 1) {
            incompatibleConfig.add("partitions");
        }
        if (config.getConsumerRebalanceListenerName().isPresent()) {
            incompatibleConfig.add("consumer-rebalance-listener");
        }
        if (!incompatibleConfig.isEmpty()) {
            log.shareGroupIncompatibleConfiguration(String.join(",", incompatibleConfig), config.getChannel());
        }

        configuration = config;
        // We cannot use vertx.getOrCreate context as it would retrieve the same one everytime.
        // It associates the context with the caller thread which will always be the same.
        // So, we force the creation of different event loop context.
        context = ((VertxInternal) vertx.getDelegate()).createEventLoopContext();
        // fire consumer event (e.g. bind metrics)
        client = new ReactiveKafkaShareConsumer<>(config, configCustomizers, deserializationFailureHandlers, consumerGroup,
                this::reportFailure, context, c -> kafkaCDIEvents.shareConsumer().fire(c));

        commitHandler = createCommitHandler(vertx);

        failureHandler = createFailureHandler();
        if (config.getHealthEnabled() || config.getHealthReadinessEnabled()) {
            health = new KafkaShareGroupSourceHealth(this, config, client, adminClientRegistry, topics);
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

        if (!config.getBatch()) {
            Multi<ConsumerRecord<K, V>> multi = client.subscribe(topics);
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
            Multi<IncomingKafkaRecord<K, V>> incomingMulti = multi.onItem()
                    .transform(rec -> new IncomingKafkaRecord<>(rec, channel, -1, commitHandler,
                            failureHandler, isCloudEventEnabled));

            incomingMulti = commitHandler.decorateStream(incomingMulti);

            AcknowledgeType defaultAckType = AcknowledgeType
                    .valueOf(config.getShareGroupFailureDeserializationAcknowledgementType().toUpperCase());
            incomingMulti = incomingMulti.onItem().transformToUni(record -> {
                Header reasonMsgHeader = record.getHeaders().lastHeader(DESERIALIZATION_FAILURE_REASON);
                if (reasonMsgHeader != null) {
                    String message = new String(reasonMsgHeader.value());
                    RecordDeserializationException reason = new RecordDeserializationException(
                            TopicPartitions.getTopicPartition(record), record.getOffset(), message, null);
                    Header ackTypeHeader = record.getHeaders().lastHeader(DESERIALIZATION_FAILURE_SHARE_ACK_TYPE);
                    ShareGroupAcknowledgement ack;
                    if (ackTypeHeader != null) {
                        ack = ShareGroupAcknowledgement.from(AcknowledgeType.valueOf(new String(ackTypeHeader.value())));
                    } else {
                        ack = ShareGroupAcknowledgement.from(defaultAckType);
                    }
                    return failureHandler.handle(record, reason, Metadata.of(ack))
                            .onItem().transform(ignore -> null);
                }
                return commitHandler.received(record);
            }).concatenate();

            if (config.getTracingEnabled()) {
                incomingMulti = incomingMulti.onItem().invoke(record -> incomingTrace(record, false));
            }
            this.stream = incomingMulti
                    .onFailure().invoke(t -> reportFailure(t, false));
            this.batchStream = null;
        } else {
            Multi<ConsumerRecords<K, V>> multi = client.subscribeBatch(topics);
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

            Multi<IncomingKafkaShareGroupRecordBatch<K, V>> incomingMulti = multi.onItem().transformToUni(rec -> {
                IncomingKafkaShareGroupRecordBatch<K, V> batch = new IncomingKafkaShareGroupRecordBatch<>(rec, channel,
                        commitHandler, failureHandler, isCloudEventEnabled);
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

        if (list != null && top != null) {
            throw ex.invalidTopics(channel, "topic");
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
    public void incomingTrace(IncomingKafkaShareGroupRecordBatch<K, V> kafkaBatchRecord) {
        if (isTracingEnabled) {
            for (KafkaRecord<K, V> record : kafkaBatchRecord.getRecords()) {
                IncomingKafkaRecord<K, V> kafkaRecord = record.unwrap(IncomingKafkaRecord.class);
                incomingTrace(kafkaRecord, true);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Uni<IncomingKafkaShareGroupRecordBatch<K, V>> receiveBatchRecord(IncomingKafkaShareGroupRecordBatch<K, V> batch) {
        List<Uni<IncomingKafkaRecord<K, V>>> records = new ArrayList<>();
        // For share groups, every record needs to be tracked (not just latest per partition)
        // because each record has its own acquisition lock that needs renewal
        for (KafkaRecord<K, V> record : batch.getRecords()) {
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

    private KafkaFailureHandler createFailureHandler() {
        AcknowledgeType defaultAckType = AcknowledgeType.valueOf(
                configuration.getShareGroupFailureAcknowledgementType().toUpperCase());
        return new KafkaShareGroupFailureHandler(client, defaultAckType);
    }

    private KafkaCommitHandler createCommitHandler(Vertx vertx) {
        int autoCommitInterval = configuration.config()
                .getOptionalValue(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Integer.class)
                .orElse(5000);
        int defaultTimeout = configuration.config()
                .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class)
                .orElse(60000);
        return new KafkaShareGroupCommit(client, vertx,
                defaultTimeout,
                autoCommitInterval,
                configuration.getShareGroupUnprocessedRecordMaxAgeMs(),
                this::reportFailure);
    }

    public Multi<IncomingKafkaRecord<K, V>> getStream() {
        return stream;
    }

    public Multi<IncomingKafkaShareGroupRecordBatch<K, V>> getBatchStream() {
        return batchStream;
    }

    public void closeQuietly() {
        try {
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
    public ReactiveKafkaShareConsumer<K, V> getConsumer() {
        return this.client;
    }

    public String getConsumerGroup() {
        return group;
    }

    public String getClientId() {
        return client.get(ConsumerConfig.CLIENT_ID_CONFIG);
    }

    public String getChannel() {
        return channel;
    }

    public boolean hasSubscribers() {
        return subscribed;
    }
}
