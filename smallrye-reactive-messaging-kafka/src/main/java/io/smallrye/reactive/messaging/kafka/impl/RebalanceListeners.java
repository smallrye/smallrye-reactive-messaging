package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.lang.reflect.Field;
import java.util.*;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

public class RebalanceListeners {

    static ConsumerRebalanceListener createRebalanceListener(
            KafkaConnectorIncomingConfiguration config,
            String consumerGroup,
            Instance<KafkaConsumerRebalanceListener> instances,
            KafkaConsumer<?, ?> consumer,
            KafkaCommitHandler commitHandler) {
        Optional<KafkaConsumerRebalanceListener> rebalanceListener = findMatchingListener(config, consumerGroup, instances);

        if (rebalanceListener.isPresent()) {
            KafkaConsumerRebalanceListener listener = rebalanceListener.get();
            return new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    long demand = consumer.demand();
                    consumer.pause();
                    log.executingConsumerRevokedRebalanceListener(consumerGroup);

                    try {
                        listener.onPartitionsRevoked(consumer.getDelegate().unwrap(), partitions);
                        log.executedConsumerRevokedRebalanceListener(consumerGroup);
                    } catch (RuntimeException e) {
                        log.unableToExecuteConsumerRevokedRebalanceListener(consumerGroup, e);
                        throw e;
                    } finally {
                        consumer.fetch(demand);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    long demand = consumer.demand();
                    consumer.pause();
                    Collection<io.vertx.kafka.client.common.TopicPartition> tps = wrap(partitions);
                    commitHandler.partitionsAssigned(tps);
                    try {
                        listener.onPartitionsAssigned(consumer.getDelegate().unwrap(), partitions);
                        log.executedConsumerAssignedRebalanceListener(consumerGroup);
                    } catch (RuntimeException e) {
                        log.reEnablingConsumerForGroup(consumerGroup);
                        throw e;
                    } finally {
                        consumer.fetch(demand);
                    }
                }
            };
        } else {
            return new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    Collection<io.vertx.kafka.client.common.TopicPartition> tps = wrap(partitions);
                    long demand = consumer.demand();
                    consumer.pause();
                    try {
                        commitHandler.partitionsRevoked(tps);
                    } finally {
                        consumer.fetch(demand);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    Collection<io.vertx.kafka.client.common.TopicPartition> tps = wrap(partitions);
                    long demand = consumer.demand();
                    consumer.pause();
                    try {
                        commitHandler.partitionsAssigned(tps);
                    } finally {
                        consumer.fetch(demand);
                    }
                }
            };
        }
    }

    private static Collection<io.vertx.kafka.client.common.TopicPartition> wrap(Collection<TopicPartition> partitions) {
        List<io.vertx.kafka.client.common.TopicPartition> tps = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions) {
            tps.add(new io.vertx.kafka.client.common.TopicPartition(partition.topic(), partition.partition()));
        }
        return tps;
    }

    private static Optional<KafkaConsumerRebalanceListener> findMatchingListener(
            KafkaConnectorIncomingConfiguration config, String consumerGroup,
            Instance<KafkaConsumerRebalanceListener> instances) {
        return config.getConsumerRebalanceListenerName()
                .map(name -> {
                    log.loadingConsumerRebalanceListenerFromConfiguredName(name);
                    Instance<KafkaConsumerRebalanceListener> matching = instances.select(Identifier.Literal.of(name));
                    if (matching.isUnsatisfied()) {
                        // this `if` block should be removed when support for the `@Named` annotation is removed
                        matching = instances.select(NamedLiteral.of(name));
                    }
                    // We want to fail if a name if set, but no match or too many matches
                    if (matching.isUnsatisfied()) {
                        throw KafkaExceptions.ex.unableToFindRebalanceListener(name, config.getChannel());
                    } else if (matching.stream().count() > 1) {
                        throw KafkaExceptions.ex.unableToFindRebalanceListener(name, config.getChannel(),
                                (int) matching.stream().count());
                    } else if (matching.stream().count() == 1) {
                        return Optional.of(matching.get());
                    } else {
                        return Optional.<KafkaConsumerRebalanceListener> empty();
                    }
                })
                .orElseGet(() -> {
                    Instance<KafkaConsumerRebalanceListener> matching = instances.select(Identifier.Literal.of(consumerGroup));
                    if (matching.isUnsatisfied()) {
                        // this `if` block should be removed when support for the `@Named` annotation is removed
                        matching = instances.select(NamedLiteral.of(consumerGroup));
                    }
                    if (!matching.isUnsatisfied()) {
                        log.loadingConsumerRebalanceListenerFromGroupId(consumerGroup);
                        return Optional.of(matching.get());
                    }
                    return Optional.empty();
                });
    }

    /**
     * HACK - inject the listener using reflection to replace the one set by vert.x
     *
     * @param consumer the consumer
     * @param listener the listener
     */
    @SuppressWarnings("rawtypes")
    public static void inject(KafkaConsumer<?, ?> consumer, ConsumerRebalanceListener listener) {
        KafkaReadStream readStream = consumer.getDelegate().asStream();
        if (readStream instanceof KafkaReadStreamImpl) {
            try {
                Field field = readStream.getClass().getDeclaredField("rebalanceListener");
                field.setAccessible(true);
                field.set(readStream, listener);
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot inject rebalance listener", e);
            }
        } else {
            throw new IllegalArgumentException("Cannot inject rebalance listener - not a Kafka Read Stream");
        }
    }
}
