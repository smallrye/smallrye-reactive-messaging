package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaShareConsumer;
import io.smallrye.reactive.messaging.kafka.fault.DeserializerWrapper;
import io.smallrye.reactive.messaging.providers.helpers.ConfigUtils;
import io.smallrye.reactive.messaging.providers.helpers.PausablePollingStream;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.vertx.core.Context;

public class ReactiveKafkaShareConsumer<K, V> implements KafkaShareConsumer<K, V> {

    private final AtomicBoolean closed = new AtomicBoolean(true);

    /**
     * Avoid concurrent call to `poll`
     */
    private final Uni<ShareConsumer<K, V>> consumerUni;
    private final AtomicReference<ShareConsumer<K, V>> consumerRef = new AtomicReference<>();
    private final RuntimeKafkaSourceConfiguration configuration;
    private final Duration pollTimeout;
    private final String shareGroup;
    private final String clientId;

    private final ScheduledExecutorService kafkaWorker;
    private final PausablePollingStream<ConsumerRecords<K, V>, ConsumerRecord<K, V>> stream;
    private final PausablePollingStream<ConsumerRecords<K, V>, ConsumerRecords<K, V>> batchStream;
    private final Map<String, Object> kafkaConfiguration;
    private final Context context;

    public ReactiveKafkaShareConsumer(KafkaConnectorIncomingConfiguration config,
            Instance<ClientCustomizer<Map<String, Object>>> configCustomizers,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            String shareGroup,
            BiConsumer<Throwable, Boolean> reportFailure,
            Context context,
            java.util.function.Consumer<ShareConsumer<K, V>> onConsumerCreated) {
        this(getKafkaShareConsumerConfiguration(config, configCustomizers, shareGroup),
                createDeserializationFailureHandler(true, deserializationFailureHandlers, config),
                createDeserializationFailureHandler(false, deserializationFailureHandlers, config),
                RuntimeKafkaSourceConfiguration.buildFromConfiguration(config),
                config.getLazyClient(),
                config.getPollTimeout(),
                config.getFailOnDeserializationFailure(),
                config.getCloudEvents(),
                onConsumerCreated,
                reportFailure,
                context);
    }

    public ReactiveKafkaShareConsumer(Map<String, Object> kafkaConfiguration,
            DeserializationFailureHandler<K> keyDeserializationFailureHandler,
            DeserializationFailureHandler<V> valueDeserializationFailureHandler,
            RuntimeKafkaSourceConfiguration config,
            boolean lazyClient,
            int pollTimeout,
            boolean failOnDeserializationFailure,
            boolean cloudEventsEnabled,
            java.util.function.Consumer<ShareConsumer<K, V>> onConsumerCreated,
            BiConsumer<Throwable, Boolean> reportFailure,
            Context context) {
        this.configuration = config;
        this.kafkaConfiguration = kafkaConfiguration;

        String keyDeserializerCN = (String) kafkaConfiguration.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        String valueDeserializerCN = (String) kafkaConfiguration.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        this.shareGroup = (String) kafkaConfiguration.get(ConsumerConfig.GROUP_ID_CONFIG);
        this.clientId = (String) kafkaConfiguration.get(ConsumerConfig.CLIENT_ID_CONFIG);

        if (valueDeserializerCN == null) {
            throw ex.missingValueDeserializer(config.getChannel(), config.getChannel());
        }

        Deserializer<K> keyDeserializer = new DeserializerWrapper<>(keyDeserializerCN, true,
                keyDeserializationFailureHandler, reportFailure, failOnDeserializationFailure, cloudEventsEnabled);
        Deserializer<V> valueDeserializer = new DeserializerWrapper<>(valueDeserializerCN, false,
                valueDeserializationFailureHandler, reportFailure, failOnDeserializationFailure, cloudEventsEnabled);

        // Configure the underlying deserializers
        keyDeserializer.configure(kafkaConfiguration, true);
        valueDeserializer.configure(kafkaConfiguration, false);

        this.pollTimeout = Duration.ofMillis(pollTimeout);

        kafkaWorker = Executors.newSingleThreadScheduledExecutor(KafkaPollingThread::new);

        stream = new PausablePollingStream<>(clientId, this.poll(), (records, p) -> {
            for (ConsumerRecord<K, V> r : records) {
                p.onNext(r);
            }
        }, kafkaWorker, 1, false);
        batchStream = new PausablePollingStream<>(clientId, this.poll(), (records, p) -> p.onNext(records),
                kafkaWorker, 1, false);

        consumerUni = Uni.createFrom().item(() -> consumerRef.updateAndGet(c -> {
            if (c != null) {
                return c;
            } else {
                ShareConsumer<K, V> consumer = new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(
                        kafkaConfiguration, keyDeserializer, valueDeserializer);
                onConsumerCreated.accept(consumer);
                closed.set(false);
                return consumer;
            }
        })).memoize().until(closed::get)
                .runSubscriptionOn(kafkaWorker);
        if (!lazyClient) {
            consumerUni.await().indefinitely();
        }
        this.context = context;
    }

    public Uni<ShareConsumer<K, V>> withConsumerOnPollingThread() {
        return consumerUni;
    }

    public String getShareGroup() {
        return shareGroup;
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    @CheckReturnValue
    public <T> Uni<T> runOnPollingThread(Function<ShareConsumer<K, V>, T> action) {
        return withConsumerOnPollingThread().map(action);
    }

    @Override
    @CheckReturnValue
    public Uni<Void> runOnPollingThread(java.util.function.Consumer<ShareConsumer<K, V>> action) {
        return withConsumerOnPollingThread()
                .invoke(action)
                .replaceWithVoid();
    }

    @SuppressWarnings("unchecked")
    Uni<ConsumerRecords<K, V>> poll() {
        return Uni.createFrom().item(consumerRef::get)
                .map(c -> c.poll(pollTimeout))
                .runSubscriptionOn(kafkaWorker)
                .onFailure(WakeupException.class).recoverWithItem((ConsumerRecords<K, V>) ConsumerRecords.EMPTY)
                .onFailure(IllegalStateException.class).retry().withBackOff(Duration.ofMillis(100)).indefinitely()
                .plug(e -> {
                    if (configuration.getRetry()) {
                        int maxWait = configuration.getRetryMaxWait();
                        int retryAttempts = configuration.getRetryAttempts() == -1 ? Integer.MAX_VALUE
                                : configuration.getRetryAttempts();
                        return e
                                .onFailure().invoke(f -> log.pollFailureRetry(shareGroup, configuration.getChannel(), f))
                                .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                                .atMost(retryAttempts);
                    }
                    return e;
                });
    }

    @CheckReturnValue
    public Multi<ConsumerRecord<K, V>> subscribe(Set<String> topics) {
        return stream.getStream().onSubscription().call(() -> withConsumerOnPollingThread()
                .invoke(c -> c.subscribe(topics))
                .runSubscriptionOn(kafkaWorker)
                .replaceWithVoid())
                .emitOn(r -> context.runOnContext(x -> r.run()));
    }

    @CheckReturnValue
    public Multi<ConsumerRecords<K, V>> subscribeBatch(Set<String> topics) {
        return batchStream.getStream().onSubscription().call(() -> withConsumerOnPollingThread()
                .invoke(c -> c.subscribe(topics))
                .runSubscriptionOn(kafkaWorker)
                .replaceWithVoid())
                .emitOn(r -> context.runOnContext(x -> r.run()));
    }

    private static Map<String, Object> getKafkaShareConsumerConfiguration(KafkaConnectorIncomingConfiguration configuration,
            Instance<ClientCustomizer<Map<String, Object>>> configInterceptors,
            String shareGroup) {
        Map<String, Object> map = new HashMap<>();
        JsonHelper.asJsonObject(configuration.config())
                .forEach(e -> map.put(e.getKey(), e.getValue().toString()));
        map.put(ConsumerConfig.GROUP_ID_CONFIG, shareGroup);
        map.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
                ShareAcknowledgementMode.AcknowledgementMode.EXPLICIT.name().toLowerCase());

        if (!map.containsKey(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // If no backoff is set, use 10s, it avoids high load on disconnection.
            map.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }

        String servers = configuration.getBootstrapServers();
        if (!map.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.configServers(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!map.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            log.keyDeserializerOmitted();
            map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, configuration.getKeyDeserializer());
        }

        // Share groups don't support enable.auto.commit and auto.offset.reset - remove them if present
        map.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        map.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);

        // Consumer id generation:
        // 1. If no client id set in the config, set it to channel name, the prefix default value is "kafka-share-consumer-",
        // 1. If a client id set in the config, prefix with the default value "",
        // In any case if consumer index is -1, suffix is "", otherwise, suffix the index.

        map.compute(ConsumerConfig.CLIENT_ID_CONFIG, (k, configured) -> {
            if (configured == null) {
                String prefix = configuration.getClientIdPrefix().orElse("kafka-share-consumer-");
                // Case 1
                return prefix + configuration.getChannel();
            } else {
                String prefix = configuration.getClientIdPrefix().orElse("");
                // Case 2
                return prefix + configured;
            }
        });

        ConfigurationCleaner.cleanupConsumerConfiguration(map);

        return ConfigUtils.customize(configuration.config(), configInterceptors, map);
    }

    @SuppressWarnings({ "unchecked" })
    public static <T> DeserializationFailureHandler<T> createDeserializationFailureHandler(boolean isKey,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            KafkaConnectorIncomingConfiguration configuration) {
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

    public String get(String attribute) {
        return (String) kafkaConfiguration.get(attribute);
    }

    @Override
    public ShareConsumer<K, V> unwrap() {
        return consumerRef.get();
    }

    @Override
    @CheckReturnValue
    public Uni<Void> commit() {
        return withConsumerOnPollingThread()
                .invoke(ShareConsumer::commitSync)
                .replaceWithVoid();
    }

    @CheckReturnValue
    @Override
    public Uni<Void> commitAsync() {
        return withConsumerOnPollingThread().invoke(ShareConsumer::commitAsync).replaceWithVoid();
    }

    @CheckReturnValue
    @Override
    public Uni<Void> acknowledge(ConsumerRecord<K, V> record) {
        return withConsumerOnPollingThread().invoke(c -> c.acknowledge(record))
                .replaceWithVoid();
    }

    @CheckReturnValue
    @Override
    public Uni<Void> acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
        return withConsumerOnPollingThread().invoke(c -> c.acknowledge(record, type))
                .replaceWithVoid();
    }

    @Override
    public Map<String, ?> configuration() {
        return kafkaConfiguration;
    }

    public void close() {
        int timeout = configuration.getCloseTimeout();
        if (closed.compareAndSet(false, true)) {
            Uni<Void> uni = Uni.createFrom().item(() -> consumerRef.get())
                    .invoke(c -> c.close(Duration.ofMillis(timeout)))
                    .runSubscriptionOn(kafkaWorker)
                    .onItem().invoke(kafkaWorker::shutdown)
                    .replaceWithVoid();

            // Interrupt polling
            ShareConsumer<K, V> consumer = consumerRef.get();
            if (consumer != null) {
                consumer.wakeup();
            }
            if (Context.isOnEventLoopThread()) {
                // We can't block, just forget the result
                uni.subscribeAsCompletionStage();
            } else {
                uni.await().atMost(Duration.ofMillis(timeout * 2L));
            }
        }
    }

    boolean isClosed() {
        return closed.get();
    }

}
