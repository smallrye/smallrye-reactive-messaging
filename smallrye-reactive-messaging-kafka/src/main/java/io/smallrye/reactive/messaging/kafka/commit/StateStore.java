package io.smallrye.reactive.messaging.kafka.commit;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * State store metadata type for injecting state store interactions into received messages.
 * This allows accessing the current processing state restored from the state store, and produce the next state.
 * The next state can be saved locally or persisted into the external store.
 *
 * <p>
 * A sample processing method with checkpointing would be:
 *
 * <pre>
 * &#64;Incoming("in")
 * public CompletionStage&lt;Void&gt; process(Message&lt;String&gt; msg) {
 *     StateStore&lt;Integer&gt; stateStore = StateStore.fromMessage(msg);
 *     if (stateStore != null) {
 *         stateStore.transformAndStoreOnAck(0, current -> current + msg.getPayload());
 *     }
 *     return CompletableFuture.completed(null);
 * }
 * </pre>
 *
 * @param <T> type of the processing state
 */
public class StateStore<T> {
    private final TopicPartition topicPartition;
    private final long recordOffset;
    private final Supplier<ProcessingState<T>> currentSupplier;
    private ProcessingState<T> next;
    private boolean persist;

    @SuppressWarnings("unchecked")
    public static <S> ProcessingState<S> getProcessingState(Message<?> message) {
        return (ProcessingState<S>) message.getMetadata(StateStore.class)
                .flatMap(StateStore::getNext).orElse(null);
    }

    public static boolean isPersist(Message<?> message) {
        return message.getMetadata(StateStore.class).map(StateStore::isPersist).orElse(false);
    }

    @SuppressWarnings("unchecked")
    public static <S> StateStore<S> fromMessage(Message<?> message) {
        return (StateStore<S>) message.getMetadata(StateStore.class).orElse(null);
    }

    public StateStore(TopicPartition topicPartition, long recordOffset, Supplier<ProcessingState<T>> stateSupplier) {
        this.topicPartition = topicPartition;
        this.recordOffset = recordOffset;
        this.currentSupplier = stateSupplier;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public long getRecordOffset() {
        return recordOffset;
    }

    public boolean isPersist() {
        return persist;
    }

    public Optional<ProcessingState<T>> getCurrent() {
        return Optional.ofNullable(currentSupplier.get());
    }

    public Optional<ProcessingState<T>> getNext() {
        return Optional.ofNullable(next);
    }

    public T storeLocal(T state, long offset) {
        this.next = new ProcessingState<>(state, offset);
        return this.next.getState();
    }

    public T storeLocal(T state) {
        return storeLocal(state, getRecordOffset() + 1);
    }

    public T transformAndStoreLocal(T initialState, Function<T, T> transformation) {
        return storeLocal(transformation.apply(getCurrent().map(ProcessingState::getState).orElse(initialState)));
    }

    public T storeOnAck(T state, long offset) {
        this.persist = true;
        return storeLocal(state, offset);
    }

    public T storeOnAck(T state) {
        return storeOnAck(state, getRecordOffset() + 1);
    }

    public T transformAndStoreOnAck(T initialState, Function<T, T> transformation) {
        return storeOnAck(transformation.apply(getCurrent().map(ProcessingState::getState).orElse(initialState)));
    }
}
