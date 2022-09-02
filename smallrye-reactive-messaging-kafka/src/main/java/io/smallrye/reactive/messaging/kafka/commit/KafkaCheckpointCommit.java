package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.commit.StateStore.getProcessingState;
import static io.smallrye.reactive.messaging.kafka.commit.StateStore.isPersist;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jboss.logging.Logger;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging;
import io.vertx.mutiny.core.Vertx;

/**
 * Abstract commit handler for checkpointing processing state on a state store
 *
 * Instead of committing topic-partition offsets back to Kafka, checkpointing commit handlers persist and restore offsets on an
 * external store.
 * It associates a {@link ProcessingState} with a topic-partition offset, and lets the processing resume from the checkpointed
 * state.
 *
 * This abstract implementation holds a local map of {@link ProcessingState} per topic-partition,
 * and ensures it is accessed on the captured Vert.x context.
 *
 * Concrete implementations provide {@link #fetchProcessingState(TopicPartition)} and
 * {@link #persistProcessingState(TopicPartition, ProcessingState)} depending on the state store.
 */
@Experimental("Experimental API")
public abstract class KafkaCheckpointCommit extends ContextHolder implements KafkaCommitHandler {

    protected KafkaLogging log = Logger.getMessageLogger(KafkaLogging.class, "io.smallrye.reactive.messaging.kafka");

    protected final Map<TopicPartition, ProcessingState<?>> processingStateMap = new HashMap<>();

    protected final KafkaConnectorIncomingConfiguration config;
    protected final KafkaConsumer<?, ?> consumer;
    protected final BiConsumer<Throwable, Boolean> reportFailure;

    public KafkaCheckpointCommit(Vertx vertx,
            KafkaConnectorIncomingConfiguration config,
            KafkaConsumer<?, ?> consumer,
            BiConsumer<Throwable, Boolean> reportFailure,
            int defaultTimeout) {
        super(vertx, defaultTimeout);
        this.config = config;
        this.consumer = consumer;
        this.reportFailure = reportFailure;
    }

    @Override
    public <K, V> Uni<IncomingKafkaRecord<K, V>> received(IncomingKafkaRecord<K, V> record) {
        return Uni.createFrom().item(record)
                .emitOn(this::runOnContext) // state map is accessed on the captured context
                .onItem().transform(r -> {
                    TopicPartition tp = new TopicPartition(record.getTopic(), record.getPartition());
                    r.injectMetadata(new StateStore<>(tp, record.getOffset(), () -> processingStateMap.get(tp)));
                    return r;
                });
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
        TopicPartition tp = new TopicPartition(record.getTopic(), record.getPartition());
        ProcessingState<?> newState = getProcessingState(record);
        boolean persist = isPersist(record);
        if (newState != null) {
            return Uni.createFrom().item(newState)
                    .emitOn(this::runOnContext) // state map is accessed on the captured context
                    .onItem().invoke(s -> processingStateMap.put(tp, s))
                    .chain(s -> persist ? persistProcessingState(tp, newState) : Uni.createFrom().voidItem())
                    .replaceWithVoid();
        }
        return Uni.createFrom().voidItem();
    }

    @Override
    public void terminate(boolean graceful) {
        consumer.getAssignments()
                .chain(this::persistStateFor)
                .emitOn(this::runOnContext) // access state map on the captured context
                .invoke(processingStateMap::clear)
                .await().atMost(Duration.ofMillis(getTimeoutInMillis()));
    }

    @Override
    public void partitionsAssigned(Collection<TopicPartition> partitions) {
        List<? extends Tuple2<TopicPartition, ? extends ProcessingState<?>>> states = Multi.createFrom().iterable(partitions)
                .emitOn(this::runOnContext) // state map is accessed on the captured context
                .onItem().transformToUniAndConcatenate(tp -> fetchProcessingState(tp).map(s -> Tuple2.of(tp, s)))
                .emitOn(this::runOnContext) // state map is accessed on the captured context
                .onItem().invoke(t -> processingStateMap.put(t.getItem1(), t.getItem2()))
                .collect().asList()
                .await().atMost(Duration.ofMillis(getTimeoutInMillis()));
        Consumer<?, ?> kafkaConsumer = consumer.unwrap();
        for (Tuple2<TopicPartition, ? extends ProcessingState<?>> tuple : states) {
            ProcessingState<?> state = tuple.getItem2();
            kafkaConsumer.seek(tuple.getItem1(), state != null ? state.getOffset() : 0L);
        }
    }

    @Override
    public void partitionsRevoked(Collection<TopicPartition> partitions) {
        persistStateFor(partitions).await().atMost(Duration.ofMillis(getTimeoutInMillis()));
    }

    private Uni<List<Void>> persistStateFor(Collection<TopicPartition> partitions) {
        return Multi.createFrom().iterable(partitions)
                .emitOn(this::runOnContext) // access state map on the captured context
                .onItem().transform(tp -> Tuple2.of(tp, processingStateMap.get(tp)))
                .skip().where(t -> t.getItem2() == null)
                .onItem().transformToUniAndConcatenate(t -> this.persistProcessingState(t.getItem1(), t.getItem2()))
                .collect().asList();
    }

    protected abstract Uni<ProcessingState<?>> fetchProcessingState(TopicPartition partition);

    protected abstract Uni<Void> persistProcessingState(TopicPartition partition, ProcessingState<?> state);

}
