package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.TRACER;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.serialization.Deserializer;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.i18n.ProviderLogging;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.commit.*;
import io.smallrye.reactive.messaging.kafka.fault.*;
import io.smallrye.reactive.messaging.kafka.health.KafkaSourceReadinessHealth;
import io.vertx.core.AsyncResult;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class KafkaSource<K, V> {
    private final Multi<IncomingKafkaRecord<K, V>> stream;
    private final KafkaConsumer<K, V> consumer;
    private final KafkaFailureHandler failureHandler;
    private final KafkaCommitHandler commitHandler;
    private final KafkaConnectorIncomingConfiguration configuration;
    private final List<Throwable> failures = new ArrayList<>();
    private final Set<String> topics;
    private final Pattern pattern;
    private final boolean isTracingEnabled;
    private final boolean isHealthEnabled;
    private final boolean isCloudEventEnabled;
    private final String channel;
    private volatile boolean subscribed;
    private final KafkaSourceReadinessHealth health;

    public KafkaSource(Vertx vertx,
            String consumerGroup,
            KafkaConnectorIncomingConfiguration config,
            Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners,
            KafkaCDIEvents kafkaCDIEvents,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            int index) {

        topics = getTopics(config);

        if (config.getPattern()) {
            pattern = Pattern.compile(config.getTopic()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid Kafka incoming configuration for channel `"
                            + config.getChannel() + "`, `pattern` must be used with the `topic` attribute")));
            log.configuredPattern(config.getChannel(), pattern.toString());
        } else {
            log.configuredTopics(config.getChannel(), topics);
            pattern = null;
        }

        Map<String, String> kafkaConfiguration = new HashMap<>();
        this.configuration = config;

        isTracingEnabled = this.configuration.getTracingEnabled();
        isHealthEnabled = this.configuration.getHealthEnabled();
        isCloudEventEnabled = this.configuration.getCloudEvents();
        channel = this.configuration.getChannel();

        JsonHelper.asJsonObject(config.config())
                .forEach(e -> kafkaConfiguration.put(e.getKey(), e.getValue().toString()));
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        if (!kafkaConfiguration.containsKey(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // If no backoff is set, use 10s, it avoids high load on disconnection.
            kafkaConfiguration.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }

        String servers = config.getBootstrapServers();
        if (!kafkaConfiguration.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.configServers(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!kafkaConfiguration.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            log.keyDeserializerOmitted();
            kafkaConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        }

        if (!kafkaConfiguration.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            log.disableAutoCommit(config.getChannel());
            kafkaConfiguration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }

        if (!kafkaConfiguration.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            String name = "kafka-consumer-" + config.getChannel();
            if (index != -1) {
                name += "-" + index;
            }
            kafkaConfiguration.put(ConsumerConfig.CLIENT_ID_CONFIG, name);
        }

        String commitStrategy = config
                .getCommitStrategy()
                .orElse(Boolean.parseBoolean(kafkaConfiguration.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                        ? KafkaCommitHandler.Strategy.IGNORE.name()
                        : KafkaCommitHandler.Strategy.THROTTLED.name());

        ConfigurationCleaner.cleanupConsumerConfiguration(kafkaConfiguration);

        Deserializer<K> keyDeserializer = new DeserializerWrapper<>(
                kafkaConfiguration.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), true,
                getDeserializationHandler(true, deserializationFailureHandlers),
                this);
        String className = kafkaConfiguration.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        if (className == null) {
            throw ex.missingValueDeserializer(config.getChannel(), config.getChannel());
        }
        Deserializer<V> valueDeserializer = new DeserializerWrapper<>(
                className, false, getDeserializationHandler(false, deserializationFailureHandlers),
                this);

        // Configure the underlying deserializers
        keyDeserializer.configure(kafkaConfiguration, true);
        valueDeserializer.configure(kafkaConfiguration, false);

        final KafkaConsumer<K, V> kafkaConsumer = createKafkaConsumer(vertx,
                ConfigurationCleaner.asKafkaConfiguration(kafkaConfiguration), keyDeserializer, valueDeserializer);

        // fire consumer event (e.g. bind metrics)
        kafkaCDIEvents.consumer().fire(kafkaConsumer.getDelegate().unwrap());

        commitHandler = createCommitHandler(vertx, kafkaConsumer, consumerGroup, config, commitStrategy);
        failureHandler = createFailureHandler(config, vertx, kafkaConfiguration, kafkaCDIEvents);
        this.consumer = kafkaConsumer;
        if (configuration.getHealthEnabled() && configuration.getHealthReadinessEnabled()) {
            health = new KafkaSourceReadinessHealth(vertx, configuration, kafkaConfiguration,
                    consumer.getDelegate().unwrap(), topics, pattern);
        } else {
            health = null;
        }

        ConsumerRebalanceListener listener = RebalanceListeners
                .createRebalanceListener(config, consumerGroup, consumerRebalanceListeners, consumer, commitHandler);
        RebalanceListeners.inject(this.consumer, listener);

        Multi<KafkaConsumerRecord<K, V>> multi = consumer.toMulti()
                .onFailure().invoke(t -> {
                    log.unableToReadRecord(topics, t);
                    reportFailure(t, false);
                });

        if (commitHandler instanceof ContextHolder) {
            // We need to capture the Vert.x context used by the Vert.x Kafka client, so we can be sure to always used
            // the same.
            ((ContextHolder) commitHandler).capture(consumer.getDelegate().asStream());
        }

        boolean retry = config.getRetry();
        if (retry) {
            int max = config.getRetryAttempts();
            int maxWait = config.getRetryMaxWait();
            if (max == -1) {
                // always retry
                multi
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                        .atMost(Long.MAX_VALUE);
            } else {
                multi = multi
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                        .atMost(max);
            }
        }

        Multi<IncomingKafkaRecord<K, V>> incomingMulti = multi
                .onCancellation().invoke(() -> subscribed = false)
                .onSubscribe().call(s -> {
                    this.consumer.exceptionHandler(t -> reportFailure(t, false));
                    if (this.pattern != null) {
                        BiConsumer<UniEmitter<?>, AsyncResult<Void>> completionHandler = (e, ar) -> {
                            if (ar.failed()) {
                                e.fail(ar.cause());
                            } else {
                                subscribed = true;
                                e.complete(null);
                            }
                        };

                        return Uni.createFrom().<Void> emitter(e -> {
                            @SuppressWarnings("unchecked")
                            io.vertx.kafka.client.consumer.KafkaConsumer<K, V> delegate = this.consumer.getDelegate();
                            delegate.subscribe(pattern, ar -> completionHandler.accept(e, ar));
                        });
                    } else {
                        return this.consumer.subscribe(topics)
                                .onItem().invoke(() -> subscribed = true);
                    }
                })
                .map(rec -> commitHandler
                        .received(
                                new IncomingKafkaRecord<>(rec, commitHandler, failureHandler, isCloudEventEnabled,
                                        isTracingEnabled)));

        if (config.getTracingEnabled()) {
            incomingMulti = incomingMulti.onItem().invoke(this::incomingTrace);
        }

        this.stream = incomingMulti
                .onFailure().invoke(t -> reportFailure(t, false));
    }

    @SuppressWarnings({ "unchecked" })
    private <T> DeserializationFailureHandler<T> getDeserializationHandler(boolean isKey,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers) {
        String name = isKey ? configuration.getKeyDeserializationFailureHandler().orElse(null)
                : configuration.getValueDeserializationFailureHandler().orElse(null);

        if (name == null) {
            return null;
        }

        Instance<DeserializationFailureHandler<?>> matching = deserializationFailureHandlers
                .select(Identifier.Literal.of(name));
        if (matching.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            matching = deserializationFailureHandlers.select(NamedLiteral.of(name));
            if (!matching.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }
        if (matching.isUnsatisfied()) {
            throw ex.unableToFindDeserializationFailureHandler(name, configuration.getChannel());
        } else if (matching.stream().count() > 1) {
            throw ex.unableToFindDeserializationFailureHandler(name, configuration.getChannel(),
                    (int) matching.stream().count());
        } else if (matching.stream().count() == 1) {
            return (DeserializationFailureHandler<T>) matching.get();
        } else {
            return null;
        }

    }

    private KafkaConsumer<K, V> createKafkaConsumer(Vertx vertx, Map<String, Object> kafkaConfiguration,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        org.apache.kafka.clients.consumer.KafkaConsumer<K, V> underlyingKafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                kafkaConfiguration, keyDeserializer, valueDeserializer);
        io.vertx.kafka.client.consumer.KafkaConsumer<K, V> bare = io.vertx.kafka.client.consumer.KafkaConsumer.create(
                vertx.getDelegate(), underlyingKafkaConsumer);
        return KafkaConsumer.newInstance(bare);
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
            if (consumer != null) {
                consumer.closeAndForget();
            }
        }
    }

    public void incomingTrace(IncomingKafkaRecord<K, V> kafkaRecord) {
        if (isTracingEnabled) {
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

            kafkaRecord.injectTracingMetadata(tracingMetadata.withSpan(span));

            span.end();
        }
    }

    private KafkaFailureHandler createFailureHandler(KafkaConnectorIncomingConfiguration config, Vertx vertx,
            Map<String, String> kafkaConfiguration, KafkaCDIEvents kafkaCDIEvents) {
        String strategy = config.getFailureStrategy();
        KafkaFailureHandler.Strategy actualStrategy = KafkaFailureHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case FAIL:
                return new KafkaFailStop(config.getChannel(), this);
            case IGNORE:
                return new KafkaIgnoreFailure(config.getChannel());
            case DEAD_LETTER_QUEUE:
                return KafkaDeadLetterQueue.create(vertx, kafkaConfiguration, config, this, kafkaCDIEvents);
            default:
                throw ex.illegalArgumentInvalidFailureStrategy(strategy);
        }

    }

    private KafkaCommitHandler createCommitHandler(
            Vertx vertx,
            KafkaConsumer<K, V> consumer,
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

    public void closeQuietly() {
        try {
            if (configuration.getGracefulShutdown()) {
                Set<TopicPartition> partitions = this.consumer.assignmentAndAwait();
                if (!partitions.isEmpty()) {
                    log.pauseAllPartitionOnTermination();
                    this.consumer.pauseAndAwait(partitions);
                    // 2 times the poll timeout - so we are sure that the last (non-empty) poll has completed.
                    grace(Duration.ofSeconds(2));

                    // If we don't have assignment, no need to wait.
                }
            }

            this.commitHandler.terminate(configuration.getGracefulShutdown());
            this.failureHandler.terminate();
            this.consumer.closeAndAwait();
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

        // If health is disable do not add anything to the builder.
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (health != null) {
            health.isReady(builder);
        }
        // If health is disable do not add anything to the builder.
    }

    /**
     * For testing purpose only
     *
     * @return get the underlying consumer.
     */
    public KafkaConsumer<K, V> getConsumer() {
        return this.consumer;
    }

    public boolean hasSubscribers() {
        return subscribed;
    }
}
