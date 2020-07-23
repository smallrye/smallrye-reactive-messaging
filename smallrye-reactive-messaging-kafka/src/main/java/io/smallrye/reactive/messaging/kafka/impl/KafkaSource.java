package io.smallrye.reactive.messaging.kafka.impl;

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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.commit.KafkaIgnoreCommit;
import io.smallrye.reactive.messaging.kafka.commit.KafkaLatestCommit;
import io.smallrye.reactive.messaging.kafka.commit.KafkaThrottledLatestProcessedCommit;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailStop;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaIgnoreFailure;
import io.vertx.core.AsyncResult;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.admin.KafkaAdminClient;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class KafkaSource<K, V> {
    private final Multi<IncomingKafkaRecord<K, V>> stream;
    private final KafkaConsumer<K, V> consumer;
    private final KafkaFailureHandler failureHandler;
    private final KafkaCommitHandler commitHandler;
    private final KafkaConnectorIncomingConfiguration configuration;
    private final KafkaAdminClient admin;
    private final List<Throwable> failures = new ArrayList<>();
    private final Set<String> topics;
    private final Pattern pattern;

    public KafkaSource(Vertx vertx,
            String group,
            KafkaConnectorIncomingConfiguration config,
            Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners) {

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

        JsonHelper.asJsonObject(config.config())
                .forEach(e -> kafkaConfiguration.put(e.getKey(), e.getValue().toString()));
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, group);

        String servers = config.getBootstrapServers();
        if (!kafkaConfiguration.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.configServers(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!kafkaConfiguration.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            log.keyDeserializerOmitted();
            kafkaConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        }

        String commitStrategy = config
                .getCommitStrategy()
                .orElse(Boolean.parseBoolean(kafkaConfiguration.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"))
                        ? KafkaCommitHandler.Strategy.IGNORE.name()
                        : KafkaCommitHandler.Strategy.LATEST.name());

        kafkaConfiguration.remove("channel-name");
        kafkaConfiguration.remove("topic");
        kafkaConfiguration.remove("topics");
        kafkaConfiguration.remove("pattern");
        kafkaConfiguration.remove("connector");
        kafkaConfiguration.remove("retry");
        kafkaConfiguration.remove("retry-attempts");
        kafkaConfiguration.remove("broadcast");
        kafkaConfiguration.remove("partitions");
        kafkaConfiguration.remove("commit-strategy");
        kafkaConfiguration.remove("consumer-rebalance-listener.name");
        kafkaConfiguration.remove("health.enabled");

        final KafkaConsumer<K, V> kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfiguration);
        commitHandler = createCommitHandler(kafkaConsumer, kafkaConfiguration, commitStrategy);

        Map<String, Object> adminConfiguration = new HashMap<>(kafkaConfiguration);
        this.admin = KafkaAdminHelper.createAdminClient(this.configuration, vertx, adminConfiguration);

        Optional<KafkaConsumerRebalanceListener> rebalanceListener = config
                .getConsumerRebalanceListenerName()
                .map(name -> {
                    log.loadingConsumerRebalanceListenerFromConfiguredName(name);
                    return NamedLiteral.of(name);
                })
                .map(consumerRebalanceListeners::select)
                .map(Instance::get)
                .map(Optional::of)
                .orElseGet(() -> {
                    Instance<KafkaConsumerRebalanceListener> rebalanceFromGroupListeners = consumerRebalanceListeners
                            .select(NamedLiteral.of(group));

                    if (!rebalanceFromGroupListeners.isUnsatisfied()) {
                        log.loadingConsumerRebalanceListenerFromGroupId(group);
                        return Optional.of(rebalanceFromGroupListeners.get());
                    }
                    return Optional.empty();
                });

        if (rebalanceListener.isPresent()) {
            KafkaConsumerRebalanceListener listener = rebalanceListener.get();
            // If the re-balance assign fails we must resume the consumer in order to force a consumer group
            // re-balance. To do so we must wait until after the poll interval time or
            // poll interval time + session timeout if group instance id is not null.
            // We will retry the re-balance consumer listener on failure using an exponential backoff until
            // we can allow the kafka consumer to do it on its own. We do this because by default it would take
            // 5 minutes for kafka to do this which is too long. With defaults consumerReEnableWaitTime would be
            // 500000 millis. We also can't simply retry indefinitely because once the consumer has been paused
            // for consumerReEnableWaitTime kafka will force a re-balance once resumed.
            final long consumerReEnableWaitTime = Long.parseLong(
                    kafkaConfiguration.getOrDefault(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"))
                    + (kafkaConfiguration.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG) == null ? 0L
                            : Long.parseLong(
                                    kafkaConfiguration.getOrDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                                            "10000")))
                    + 11_000L; // it's possible that it might expire 10 seconds before when we need it to

            kafkaConsumer.partitionsAssignedHandler(set -> {
                kafkaConsumer.pause();
                log.executingConsumerAssignedRebalanceListener(group);
                listener.onPartitionsAssigned(kafkaConsumer, set)
                        .onFailure().invoke(t -> log.unableToExecuteConsumerAssignedRebalanceListener(group, t))
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(10))
                        .expireIn(consumerReEnableWaitTime)
                        .subscribe()
                        .with(
                                a -> {
                                    log.executedConsumerAssignedRebalanceListener(group);
                                    commitHandler.partitionsAssigned(vertx.getOrCreateContext(), set);
                                    kafkaConsumer.resume();
                                },
                                t -> {
                                    log.reEnablingConsumerforGroup(group);
                                    commitHandler.partitionsAssigned(vertx.getOrCreateContext(), set);
                                    kafkaConsumer.resume();
                                });
            });

            kafkaConsumer.partitionsRevokedHandler(set -> {
                log.executingConsumerRevokedRebalanceListener(group);
                listener.onPartitionsRevoked(kafkaConsumer, set)
                        .subscribe()
                        .with(
                                a -> log.executedConsumerRevokedRebalanceListener(group),
                                t -> log.unableToExecuteConsumerRevokedRebalanceListener(group, t));
            });
        } else {
            kafkaConsumer.partitionsAssignedHandler(set -> commitHandler.partitionsAssigned(vertx.getOrCreateContext(), set));
        }

        this.consumer = kafkaConsumer;

        failureHandler = createFailureHandler(config, vertx, kafkaConfiguration);

        Multi<KafkaConsumerRecord<K, V>> multi = consumer.toMulti()
                .onFailure().invoke(t -> {
                    log.unableToReadRecord(topics, t);
                    reportFailure(t);
                });

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

        this.stream = multi
                .onSubscribe().invokeUni(s -> {
                    this.consumer.exceptionHandler(this::reportFailure);
                    if (this.pattern != null) {
                        BiConsumer<UniEmitter<?>, AsyncResult<Void>> completionHandler = (e, ar) -> {
                            if (ar.failed()) {
                                e.fail(ar.cause());
                            } else {
                                e.complete(null);
                            }
                        };

                        return Uni.createFrom().<Void> emitter(e -> {
                            @SuppressWarnings("unchecked")
                            io.vertx.kafka.client.consumer.KafkaConsumer<K, V> delegate = this.consumer.getDelegate();
                            delegate.subscribe(pattern, ar -> completionHandler.accept(e, ar));
                        });
                    } else {
                        return this.consumer.subscribe(topics);
                    }
                })
                .map(rec -> new IncomingKafkaRecord<>(rec, commitHandler, failureHandler, config.getCloudEvents()))
                .onFailure().invoke(this::reportFailure);
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

    public synchronized void reportFailure(Throwable failure) {
        log.failureReported(topics, failure);
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);
    }

    private KafkaFailureHandler createFailureHandler(KafkaConnectorIncomingConfiguration config, Vertx vertx,
            Map<String, String> kafkaConfiguration) {
        String strategy = config.getFailureStrategy();
        KafkaFailureHandler.Strategy actualStrategy = KafkaFailureHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case FAIL:
                return new KafkaFailStop(config.getChannel(), this);
            case IGNORE:
                return new KafkaIgnoreFailure(config.getChannel());
            case DEAD_LETTER_QUEUE:
                return KafkaDeadLetterQueue.create(vertx, kafkaConfiguration, config, this);
            default:
                throw ex.illegalArgumentInvalidFailureStrategy(strategy);
        }

    }

    private KafkaCommitHandler createCommitHandler(KafkaConsumer<K, V> consumer,
            Map<String, String> config,
            String strategy) {
        KafkaCommitHandler.Strategy actualStrategy = KafkaCommitHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case LATEST:
                return new KafkaLatestCommit(consumer);
            case IGNORE:
                return new KafkaIgnoreCommit();
            case THROTTLED:
                return KafkaThrottledLatestProcessedCommit.create(consumer, config, this);
            default:
                throw ex.illegalArgumentInvalidCommitStrategy(strategy);
        }
    }

    public Multi<IncomingKafkaRecord<K, V>> getStream() {
        return stream;
    }

    public void closeQuietly() {
        try {
            this.consumer.closeAndAwait();
        } catch (Throwable e) {
            log.exceptionOnClose(e);
        }
    }

    public void isAlive(HealthReport.HealthReportBuilder builder) {
        if (configuration.getHealthEnabled()) {
            List<Throwable> actualFailures;
            synchronized (this) {
                actualFailures = new ArrayList<>(failures);
            }
            if (!actualFailures.isEmpty()) {
                builder.add(configuration.getChannel(), false,
                        actualFailures.stream().map(Throwable::getMessage).collect(Collectors.joining()));
            } else {
                builder.add(configuration.getChannel(), true);
            }
        }

        // If health is disable do not add anything to the builder.
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (configuration.getHealthEnabled()) {
            Set<String> existingTopics;
            try {
                existingTopics = admin.listTopics()
                        .await().atMost(Duration.ofSeconds(2));
                if (pattern == null && existingTopics.containsAll(topics)) {
                    builder.add(configuration.getChannel(), true);
                } else if (pattern != null) {
                    // Check that at least one topic matches
                    boolean ok = existingTopics.stream()
                            .anyMatch(s -> pattern.matcher(s).matches());
                    if (ok) {
                        builder.add(configuration.getChannel(), ok);
                    } else {
                        builder.add(configuration.getChannel(), false,
                                "Unable to find a topic matching the given pattern: " + pattern);
                    }
                } else {
                    String missing = topics.stream().filter(s -> !existingTopics.contains(s))
                            .collect(Collectors.joining());
                    builder.add(configuration.getChannel(), false, "Unable to find topic(s): " + missing);
                }
            } catch (Exception failed) {
                builder.add(configuration.getChannel(), false, "No response from broker for channel "
                        + configuration.getChannel() + " : " + failed);
            }
        }

        // If health is disable do not add anything to the builder.
    }
}
