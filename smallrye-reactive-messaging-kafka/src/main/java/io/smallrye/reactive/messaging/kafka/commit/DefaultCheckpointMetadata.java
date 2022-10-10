package io.smallrye.reactive.messaging.kafka.commit;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Default implementation of {@link CheckpointMetadata}
 *
 * @param <T> type of the processing state
 */
public class DefaultCheckpointMetadata<T> implements CheckpointMetadata<T> {

    private final TopicPartition topicPartition;
    private final long recordOffset;
    private final KafkaCheckpointCommit.CheckpointState<T> checkpointState;
    private volatile ProcessingState<T> next;
    private volatile boolean persistOnAck;

    @SuppressWarnings("unchecked")
    public static <S> DefaultCheckpointMetadata<S> fromMessage(Message<?> message) {
        return (DefaultCheckpointMetadata<S>) message.getMetadata().get(DefaultCheckpointMetadata.class).orElse(null);
    }

    public DefaultCheckpointMetadata(TopicPartition topicPartition, long recordOffset,
            KafkaCheckpointCommit.CheckpointState<T> checkpointState) {
        this.topicPartition = topicPartition;
        this.recordOffset = recordOffset;
        this.checkpointState = checkpointState;
    }

    KafkaCheckpointCommit.CheckpointState<T> getCheckpointState() {
        return checkpointState;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public long getRecordOffset() {
        return recordOffset;
    }

    public boolean isPersistOnAck() {
        return persistOnAck;
    }

    public ProcessingState<T> getCurrent() {
        return checkpointState.getProcessingState();
    }

    public ProcessingState<T> getNext() {
        return next;
    }

    @Override
    public T setNext(T state, boolean persistOnAck) {
        return transform(getCurrent().getState(), s -> state, persistOnAck);
    }

    @Override
    public T setNext(T state) {
        return transform(getCurrent().getState(), s -> state);
    }

    @Override
    public T transform(Supplier<T> initialStateSupplier, Function<T, T> transformation, boolean persistOnAck) {
        this.persistOnAck = persistOnAck;
        return checkpointState.transformState(() -> new ProcessingState<>(initialStateSupplier.get(), getRecordOffset()),
                state -> {
                    this.next = new ProcessingState<>(transformation.apply(state.getState()), getRecordOffset() + 1);
                    return next;
                }).getState();
    }

    @Override
    public T transform(T initialState, Function<T, T> transformation, boolean persistOnAck) {
        return transform(() -> initialState, transformation, persistOnAck);
    }

    @Override
    public T transform(Supplier<T> initialStateSupplier, Function<T, T> transformation) {
        return transform(initialStateSupplier, transformation, false);
    }

    @Override
    public T transform(T initialState, Function<T, T> transformation) {
        return transform(() -> initialState, transformation, false);
    }
}
