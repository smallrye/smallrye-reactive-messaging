package io.smallrye.reactive.messaging.kafka.commit;

/**
 * Checkpoint state associated with an offset.
 *
 * This object can be used to persist the processing state per topic-partition into a state store.
 *
 * @param <T> type of the processing state
 */
public class ProcessingState<T> {

    private T state;
    private long offset;

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
    public String toString() {
        return "ProcessingState{" +
                "state=" + state +
                ", offset=" + offset +
                '}';
    }
}
