package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.*;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;

public class RebalanceListeners {

    static ConsumerRebalanceListener createRebalanceListener(
            ReactiveKafkaConsumer<?, ?> reactiveKafkaConsumer,
            KafkaConnectorIncomingConfiguration config,
            String consumerGroup,
            Instance<KafkaConsumerRebalanceListener> instances,
            KafkaCommitHandler commitHandler) {
        Optional<KafkaConsumerRebalanceListener> rebalanceListener = findMatchingListener(config, consumerGroup, instances);

        if (rebalanceListener.isPresent()) {
            KafkaConsumerRebalanceListener listener = rebalanceListener.get();
            return new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.executingConsumerRevokedRebalanceListener(consumerGroup);
                    try {
                        reactiveKafkaConsumer.removeFromQueueRecordsFromTopicPartitions(partitions);
                        commitHandler.partitionsRevoked(partitions);
                        listener.onPartitionsRevoked(reactiveKafkaConsumer.unwrap(), partitions);
                        log.executedConsumerRevokedRebalanceListener(consumerGroup);
                    } catch (RuntimeException e) {
                        log.unableToExecuteConsumerRevokedRebalanceListener(consumerGroup, e);
                        throw e;
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    try {
                        if (reactiveKafkaConsumer.isPaused()) {
                            reactiveKafkaConsumer.unwrap().pause(partitions);
                        }
                        commitHandler.partitionsAssigned(partitions);
                        listener.onPartitionsAssigned(reactiveKafkaConsumer.unwrap(), partitions);
                        log.executedConsumerAssignedRebalanceListener(consumerGroup);
                    } catch (RuntimeException e) {
                        log.reEnablingConsumerForGroup(consumerGroup);
                        throw e;
                    }
                }
            };
        } else {
            return new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    reactiveKafkaConsumer.removeFromQueueRecordsFromTopicPartitions(partitions);
                    commitHandler.partitionsRevoked(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (reactiveKafkaConsumer.isPaused()) {
                        reactiveKafkaConsumer.unwrap().pause(partitions);
                    }
                    commitHandler.partitionsAssigned(partitions);
                }
            };
        }
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
                        if (!matching.isUnsatisfied()) {
                            ProviderLogging.log.deprecatedNamed();
                        }
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
                        if (!matching.isUnsatisfied()) {
                            ProviderLogging.log.deprecatedNamed();
                        }
                    }
                    if (!matching.isUnsatisfied()) {
                        log.loadingConsumerRebalanceListenerFromGroupId(consumerGroup);
                        return Optional.of(matching.get());
                    }
                    return Optional.empty();
                });
    }

}
