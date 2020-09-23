package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.enterprise.inject.AmbiguousResolutionException;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.UnsatisfiedResolutionException;
import javax.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

public class RebalanceListeners {

    static ConsumerRebalanceListener createRebalanceListener(
            Vertx vertx,
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
                    log.executingConsumerRevokedRebalanceListener(consumerGroup);

                    try {
                        listener.onPartitionsRevoked(consumer.getDelegate().unwrap(), partitions);
                        log.executedConsumerRevokedRebalanceListener(consumerGroup);
                    } catch (RuntimeException e) {
                        log.unableToExecuteConsumerRevokedRebalanceListener(consumerGroup, e);
                        throw e;
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    final long currentDemand = consumer.demand();
                    consumer.pause(); // TODO Do we need to pause all?
                    Collection<io.vertx.kafka.client.common.TopicPartition> tps = wrap(partitions);
                    commitHandler.partitionsAssigned(tps);
                    try {
                        listener.onPartitionsAssigned(consumer.getDelegate().unwrap(), partitions);
                        log.executedConsumerAssignedRebalanceListener(consumerGroup);
                    } catch (RuntimeException e) {
                        log.reEnablingConsumerforGroup(consumerGroup);
                        throw e;
                    } finally {
                        consumer.fetch(currentDemand);
                    }
                }
            };
        } else {
            return new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // TODO throlling and latest must commit sync
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    Collection<io.vertx.kafka.client.common.TopicPartition> tps = wrap(partitions);
                    commitHandler.partitionsAssigned(tps);
                }
            };
        }
    }

    private static Set<io.vertx.kafka.client.common.TopicPartition> wrap(Collection<TopicPartition> partitions) {
        return partitions.stream().map(tp -> new io.vertx.kafka.client.common.TopicPartition(tp.topic(), tp.partition()))
                .collect(Collectors.toSet());
    }

    private static Optional<KafkaConsumerRebalanceListener> findMatchingListener(
            KafkaConnectorIncomingConfiguration config, String consumerGroup,
            Instance<KafkaConsumerRebalanceListener> instances) {
        return config.getConsumerRebalanceListenerName()
                .map(name -> {
                    log.loadingConsumerRebalanceListenerFromConfiguredName(name);
                    Instance<KafkaConsumerRebalanceListener> matching = instances.select(NamedLiteral.of(name));
                    // We want to fail if a name if set, but no match or too many matches
                    if (matching.isUnsatisfied()) {
                        // TODO Extract
                        throw new UnsatisfiedResolutionException(
                                "Unable to find the rebalance listener " + name + " for channel " + config.getChannel());
                    } else if (matching.stream().count() > 1) {
                        // TODO Extract
                        throw new AmbiguousResolutionException(
                                "Unable to select the rebalance listener " + name + " for channel "
                                        + config.getChannel() + " - too many matches");
                    } else if (matching.stream().count() == 1) {
                        return Optional.of(matching.get());
                    } else {
                        return Optional.<KafkaConsumerRebalanceListener> empty();
                    }
                })
                .orElseGet(() -> {
                    Instance<KafkaConsumerRebalanceListener> matching = instances.select(NamedLiteral.of(consumerGroup));
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
