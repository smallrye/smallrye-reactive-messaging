package io.smallrye.reactive.messaging.kafka.commit;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Checkpoint metadata type for injecting state checkpointing interactions into received messages.
 * This allows accessing the current processing state restored from the state store, and produce the next state.
 * The next state can be saved locally or persisted into the external store.
 *
 * <p>
 * A sample processing method with checkpointing would be:
 *
 * <pre>
 * &#64;Incoming("in")
 * public CompletionStage&lt;Void&gt; process(Message&lt;String&gt; msg) {
 *     CheckpointMetadata&lt;Integer&gt; checkpoint = CheckpointMetadata.fromMessage(msg);
 *     if (checkpoint != null) {
 *         checkpoint.transform(0, current -> current + msg.getPayload());
 *     }
 *     return CompletableFuture.completed(null);
 * }
 * </pre>
 *
 * @param <T> type of the processing state
 */
public interface CheckpointMetadata<T> {

    @SuppressWarnings("unchecked")
    static <S> CheckpointMetadata<S> fromMessage(Message<?> message) {
        return message.getMetadata(CheckpointMetadata.class).orElse(null);
    }

    /**
     * @return the topic-partition of this record
     */
    TopicPartition getTopicPartition();

    /**
     * @return the offset of this record
     */
    long getRecordOffset();

    /**
     * @return {@code true} if the processing state will be persisted on message acknowledgement
     */
    boolean isPersistOnAck();

    /**
     * @return the current processing state
     */
    ProcessingState<T> getCurrent();

    /**
     * @return the next processing state set during this processing
     */
    ProcessingState<T> getNext();

    /**
     * Set the next processing state to the given state, associated with the current record offset + 1.
     *
     * @param state the next state
     * @param persistOnAck If {@code true} then the state will be persisted on message acknowledgement
     * @return the previous state
     */
    T setNext(T state, boolean persistOnAck);

    /**
     * Set the next processing state to the given state, associated with the current record offset + 1.
     * The state will not be persisted on message acknowledgement.
     *
     * @param state the next state
     * @return the previous state
     */
    T setNext(T state);

    /**
     * Apply the transformation to the current state, if a previous state doesn't exist, start the transformation from
     * the supplied initial state. The state will be associated with the current record offset + 1.
     *
     * @param initialStateSupplier if no previous state does exist, start the transformation from this state.
     * @param transformation the transformation function
     * @param persistOnAck If {@code true} then the state will be persisted on message acknowledgement
     * @return the previous state
     */
    T transform(Supplier<T> initialStateSupplier, Function<T, T> transformation, boolean persistOnAck);

    /**
     * See {@link #transform(Supplier, Function, boolean)}
     *
     * @param initialState if no previous state does exist, start the transformation from this state.
     * @param transformation the transformation function
     * @param persistOnAck If {@code true} then the state will be persisted on message acknowledgement
     * @return the previous state
     */
    T transform(T initialState, Function<T, T> transformation, boolean persistOnAck);

    /**
     * See {@link #transform(Supplier, Function, boolean)}
     * The state will not be persisted on message acknowledgement.
     *
     * @param initialStateSupplier if no previous state does exist, start the transformation from this state.
     * @param transformation the transformation function
     * @return the previous state
     */
    T transform(Supplier<T> initialStateSupplier, Function<T, T> transformation);

    /**
     * See {@link #transform(Supplier, Function, boolean)}
     * The state will not be persisted on message acknowledgement.
     *
     * @param initialState if no previous state does exist, start the transformation from this state.
     * @param transformation the transformation function
     * @return the previous state
     */
    T transform(T initialState, Function<T, T> transformation);
}
