package io.smallrye.reactive.messaging.kafka;

import io.smallrye.reactive.messaging.kafka.commit.FileCheckpointStateStore;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCheckpointCommit;
import io.smallrye.reactive.messaging.kafka.commit.KafkaIgnoreCommit;
import io.smallrye.reactive.messaging.kafka.commit.KafkaLatestCommit;
import io.smallrye.reactive.messaging.kafka.commit.KafkaThrottledLatestProcessedCommit;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailStop;
import io.smallrye.reactive.messaging.kafka.fault.KafkaIgnoreFailure;
import io.smallrye.reactive.messaging.kafka.impl.KafkaClientServiceImpl;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactionsFactory;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.Extension;
import java.util.Set;

public class KafkaCDIExtension implements Extension {

    private static final Set<Class<?>> DEFAULT_BEAN_CLASSES = Set.of(
        KafkaTransactionsFactory.class,
        KafkaThrottledLatestProcessedCommit.Factory.class,
        KafkaLatestCommit.Factory.class,
        KafkaIgnoreCommit.Factory.class,
        KafkaCheckpointCommit.Factory.class,
        FileCheckpointStateStore.Factory.class,
        KafkaFailStop.Factory.class,
        KafkaIgnoreFailure.Factory.class,
        KafkaDeadLetterQueue.Factory.class,
        KafkaCDIEvents.class,
        KafkaConnector.class,
        KafkaClientServiceImpl.class
    );

    void addDefaultBeans(@Observes BeforeBeanDiscovery bbd, BeanManager bm) {
        for (Class<?> clazz : DEFAULT_BEAN_CLASSES) {
            bbd.addAnnotatedType(bm.createAnnotatedType(clazz), clazz.getName());
        }
    }
}
