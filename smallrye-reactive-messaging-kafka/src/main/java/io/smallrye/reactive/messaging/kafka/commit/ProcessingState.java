package io.smallrye.reactive.messaging.kafka.commit;

import java.util.Objects;

/**
 * Checkpoint state associated with an offset.
 * <p>
 * This object can be used to persist the processing state per topic-partition into a state store.
 *
 * @param <T> type of the processing state
 */
public class ProcessingState<T> {

    private long offset;
    private T state;

    public static ProcessingState<?> EMPTY_STATE = new ProcessingState<>(null, 0L);

    public static boolean isEmptyOrNull(ProcessingState<?> state) {
        return state == null || EMPTY_STATE.equals(state);
    }

    public static <V> ProcessingState<V> getOrDefault(ProcessingState<V> state, ProcessingState<V> defaultValue) {
        return isEmptyOrNull(state) ? defaultValue : state;
    }

    public static <V> ProcessingState<V> getOrDefault(ProcessingState<V> state, V defaultValue) {
        return getOrDefault(state, new ProcessingState<>(defaultValue, 0L));
    }

    public static <V> ProcessingState<V> getOrEmpty(ProcessingState<V> state) {
        return getOrDefault(state, (ProcessingState<V>) EMPTY_STATE);
    }

    public ProcessingState(T state, long offset) {
        this.state = state;
        this.offset = offset;
    }

    public ProcessingState() {
    }

    public T getState() {
        return state;
    }

    public Long getOffset() {
        return offset;
    }

    public void setState(T state) {
        this.state = state;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ProcessingState<?> state1 = (ProcessingState<?>) o;
        return offset == state1.offset && Objects.equals(state, state1.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, state);
    }

    @Override
    public String toString() {
        return "ProcessingState{" +
                "offset=" + offset +
                ", state=" + state +
                '}';
    }
}
