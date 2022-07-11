package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.TRACER;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.enterprise.inject.Instance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.commit.*;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailStop;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaIgnoreFailure;
import io.smallrye.reactive.messaging.kafka.health.KafkaSourceHealth;
import io.vertx.core.impl.EventLoopContext;
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
    private final int index;
    private final Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers;
    private final Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners;
    private final ReactiveKafkaConsumer<K, V> client;
    private final EventLoopContext context;

    public KafkaSource(Vertx vertx,
            String consumerGroup,
            KafkaConnectorIncomingConfiguration config,
            Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners,
            KafkaCDIEvents kafkaCDIEvents,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            int index) {

        this.group = consumerGroup;
        this.index = index;
        this.deserializationFailureHandlers = deserializationFailureHandlers;
        this.consumerRebalanceListeners = consumerRebalanceListeners;

        topics = getTopics(config);

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
        client = new ReactiveKafkaConsumer<>(config, this);

        String commitStrategy = config
                .getCommitStrategy()
                .orElse(Boolean.parseBoolean(client.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                        ? KafkaCommitHandler.Strategy.IGNORE.name()
                        : KafkaCommitHandler.Strategy.THROTTLED.name());

        commitHandler = createCommitHandler(vertx, client, consumerGroup, config, commitStrategy);
        failureHandler = createFailureHandler(config, client.configuration(), kafkaCDIEvents);
        if (configuration.getHealthEnabled()) {
            health = new KafkaSourceHealth(this, configuration, client);
        } else {
            health = null;
        }

        isTracingEnabled = this.configuration.getTracingEnabled();
        isHealthEnabled = this.configuration.getHealthEnabled();
        isHealthReadinessEnabled = this.configuration.getHealthReadinessEnabled();
        isCloudEventEnabled = this.configuration.getCloudEvents();
        channel = this.configuration.getChannel();

        // fire consumer event (e.g. bind metrics)
        kafkaCDIEvents.consumer().fire(client.unwrap());

        if (commitHandler instanceof ContextHolder) {
            ((ContextHolder) commitHandler).capture(context);
        }
        this.client.setRebalanceListener();

        if (!config.getBatch()) {
            Multi<ConsumerRecord<K, V>> multi;
            if (pattern != null) {
                multi = client.subscribe(pattern);
            } else {
                multi = client.subscribe(topics);
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

            Multi<IncomingKafkaRecord<K, V>> incomingMulti;
            // TODO ogu hack to avoid buffering items using flatmap -> received is only implemented by throttled strategy
            if (KafkaCommitHandler.Strategy.from(commitStrategy) != KafkaCommitHandler.Strategy.THROTTLED) {
                incomingMulti = multi.onItem().transform(rec -> new IncomingKafkaRecord<>(rec, channel, index, commitHandler,
                        failureHandler, isCloudEventEnabled, isTracingEnabled));
            } else {
                incomingMulti = multi
                        .onItem().transformToUniAndConcatenate(rec -> {
                            IncomingKafkaRecord<K, V> record = new IncomingKafkaRecord<>(rec, channel, index, commitHandler,
                                    failureHandler, isCloudEventEnabled, isTracingEnabled);
                            return commitHandler.received(record);
                        });
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
                multi = client.subscribeBatch(topics);
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

            Multi<IncomingKafkaRecordBatch<K, V>> incomingMulti;
            // TODO ogu hack to avoid buffering items using flatmap -> received is only implemented by throttled strategy
            if (KafkaCommitHandler.Strategy.from(commitStrategy) != KafkaCommitHandler.Strategy.THROTTLED) {
                incomingMulti = multi.onItem().transform(rec -> new IncomingKafkaRecordBatch<>(rec, channel, index,
                        commitHandler, failureHandler, isCloudEventEnabled, isTracingEnabled));
            } else {
                incomingMulti = multi
                        .onItem().transformToUniAndConcatenate(rec -> {
                            IncomingKafkaRecordBatch<K, V> batch = new IncomingKafkaRecordBatch<>(rec, channel, index,
                                    commitHandler, failureHandler, isCloudEventEnabled, isTracingEnabled);
                            return receiveBatchRecord(batch);
                        });
            }

            if (config.getTracingEnabled()) {
                incomingMulti = incomingMulti.onItem().invoke(this::incomingTrace);
            }
            this.batchStream = incomingMulti
                    .onFailure().invoke(t -> reportFailure(t, false));
            this.stream = null;
        }

    }

    public Set<String> getSubscribedTopics() {
        return topics;
    }

    private Set<String> getTopics(KafkaConnectorIncomingConfiguration config) {
        String list = config.getTopics().orElse(null);
        String top = config.getTopic().orElse(null);
        String channel = config.getChannel();
        boolean isPattern = config.getPattern();

        if (list != null && top != null) {
            throw new IllegalArgumentException("The Kafka incoming configuration for channel `" + channel + "` cannot "
                    + "use `topics` and `topic` at the same time");
        }

        if (list != null && isPattern) {
            throw new IllegalArgumentException("The Kafka incoming configuration for channel `" + channel + "` cannot "
                    + "use `topics` and `pattern` at the same time");
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
        if (isTracingEnabled && TRACER != null) {
            TracingMetadata tracingMetadata = TracingMetadata.fromMessage(kafkaRecord).orElse(TracingMetadata.empty());

            final SpanBuilder spanBuilder = TRACER.spanBuilder(kafkaRecord.getTopic() + " receive")
                    .setSpanKind(SpanKind.CONSUMER);

            // Handle possible parent span
            final Context parentSpanContext = tracingMetadata.getPreviousContext();
            if (parentSpanContext != null) {
                spanBuilder.setParent(parentSpanContext);
            } else {
                spanBuilder.setNoParent();
            }

            final Span span = spanBuilder.startSpan();

            // Set Span attributes
            span.setAttribute(SemanticAttributes.MESSAGING_KAFKA_PARTITION, kafkaRecord.getPartition());
            span.setAttribute("offset", kafkaRecord.getOffset());
            span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "kafka");
            span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, kafkaRecord.getTopic());
            span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "topic");

            final String groupId = client.get(ConsumerConfig.GROUP_ID_CONFIG);
            final String clientId = client.get(ConsumerConfig.CLIENT_ID_CONFIG);
            span.setAttribute("messaging.consumer_id", constructConsumerId(groupId, clientId));
            span.setAttribute(SemanticAttributes.MESSAGING_KAFKA_CONSUMER_GROUP, groupId);
            if (!clientId.isEmpty()) {
                span.setAttribute(SemanticAttributes.MESSAGING_KAFKA_CLIENT_ID, clientId);
            }

            if (!insideBatch) {
                // Make available as parent for subsequent spans inside message processing
                span.makeCurrent();
            }

            kafkaRecord.injectTracingMetadata(tracingMetadata.withSpan(span));

            span.end();
        }
    }

    private String constructConsumerId(String groupId, String clientId) {
        String consumerId = groupId;
        if (!clientId.isEmpty()) {
            consumerId += " - " + clientId;
        }
        return consumerId;
    }

    @SuppressWarnings("unchecked")
    public void incomingTrace(IncomingKafkaRecordBatch<K, V> kafkaBatchRecord) {
        if (isTracingEnabled && TRACER != null) {
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
        return Uni.combine().all().unis(records).combinedWith(ignored -> batch);
    }

    private KafkaFailureHandler createFailureHandler(KafkaConnectorIncomingConfiguration config,
            Map<String, ?> kafkaConfiguration, KafkaCDIEvents kafkaCDIEvents) {
        String strategy = config.getFailureStrategy();
        KafkaFailureHandler.Strategy actualStrategy = KafkaFailureHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case FAIL:
                return new KafkaFailStop(config.getChannel(), this);
            case IGNORE:
                return new KafkaIgnoreFailure(config.getChannel());
            case DEAD_LETTER_QUEUE:
                return KafkaDeadLetterQueue.create(kafkaConfiguration, config, this, kafkaCDIEvents);
            default:
                throw ex.illegalArgumentInvalidFailureStrategy(strategy);
        }

    }

    private KafkaCommitHandler createCommitHandler(
            Vertx vertx,
            ReactiveKafkaConsumer<K, V> consumer,
            String group,
            KafkaConnectorIncomingConfiguration config,
            String strategy) {
        KafkaCommitHandler.Strategy actualStrategy = KafkaCommitHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case LATEST:
                log.commitStrategyForChannel("latest", config.getChannel());
                return new KafkaLatestCommit(vertx, configuration, consumer);
            case IGNORE:
                log.commitStrategyForChannel("ignore", config.getChannel());
                return new KafkaIgnoreCommit();
            case THROTTLED:
                log.commitStrategyForChannel("throttled", config.getChannel());
                return KafkaThrottledLatestProcessedCommit.create(vertx, consumer, group, config, this);
            default:
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
                if (this.client.runOnPollingThread(c -> {
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
