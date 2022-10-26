package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.mutiny.helpers.test.AssertSubscriber.DEFAULT_TIMEOUT;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

/**
 * Abstract task for consuming or producing Kafka records provided by the given {@link Multi}
 * <p>
 * This class leverages {@link AssertSubscriber} to subscribe itself to the given multi.
 *
 * @param <T> the type of items
 * @param <SELF> the reference to self type
 */
public abstract class KafkaTask<T, SELF extends KafkaTask<T, SELF>> implements Iterable<T>, AutoCloseable {

    /**
     * The {@link Multi} to subscribe
     */
    private final Multi<T> multi;

    /**
     * The {@link AssertSubscriber} for controlled subscription
     */
    private final AssertSubscriber<T> subscriber;

    /**
     * Create a new {@link KafkaTask}
     *
     * @param multi the multi to subscribe to
     */
    public KafkaTask(Multi<T> multi) {
        super();
        this.multi = multi;
        this.subscriber = AssertSubscriber.create(Long.MAX_VALUE);
        this.multi.subscribe(subscriber);
    }

    public Multi<T> getMulti() {
        return multi;
    }

    @Override
    public Iterator<T> iterator() {
        return getRecords().iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return getRecords().spliterator();
    }

    protected SELF self() {
        return (SELF) this;
    }

    /**
     * @return get the number of records received.
     */
    public long count() {
        return subscriber.getItems().size();
    }

    /**
     * @return get the first record received, potentially {@code null} if no records have been received.
     */
    public T getFirstRecord() {
        List<T> records = subscriber.getItems();
        if (records.isEmpty()) {
            return null;
        } else {
            return records.get(0);
        }
    }

    /**
     * @return get the last record received, potentially {@code null} if no records have been received.
     */
    public T getLastRecord() {
        return subscriber.getLastItem();
    }

    /**
     * The list of items that have been received.
     *
     * @return the list
     */
    public List<T> getRecords() {
        return subscriber.getItems();
    }

    private void throwFailureForCause(Runnable runnable) {
        try {
            runnable.run();
        } catch (AssertionError e) {
            if (subscriber.getFailure() != null) {
                throw new AssertionError(e.getMessage(), e);
            } else {
                throw e;
            }
        }
    }

    /**
     * Delegates to {@link AssertSubscriber#awaitNextItem()}
     *
     * @return self
     */
    public SELF awaitNextRecord() {
        return awaitNextRecord(DEFAULT_TIMEOUT);
    }

    /**
     * Delegates to {@link AssertSubscriber#awaitNextItem(Duration)}
     *
     * @return self
     */
    public SELF awaitNextRecord(Duration duration) {
        throwFailureForCause(() -> subscriber.awaitNextItem(duration));
        return self();
    }

    /**
     * Delegates to {@link AssertSubscriber#awaitNextItems(int)}
     *
     * @return self
     */
    public SELF awaitNextRecords(int number) {
        return awaitNextRecords(number, DEFAULT_TIMEOUT);
    }

    /**
     * Delegates to {@link AssertSubscriber#awaitNextItems(int, Duration)}
     *
     * @return self
     */
    public SELF awaitNextRecords(int number, Duration duration) {
        throwFailureForCause(() -> subscriber.awaitNextItems(number, duration));
        return self();
    }

    /**
     * Delegates to {@link AssertSubscriber#awaitItems(int)}
     *
     * @return self
     */
    public SELF awaitRecords(int number) {
        return awaitRecords(number, DEFAULT_TIMEOUT);
    }

    /**
     * Delegates to {@link AssertSubscriber#awaitItems(int, Duration)}
     *
     * @return self
     */
    public SELF awaitRecords(int number, Duration duration) {
        throwFailureForCause(() -> subscriber.awaitItems(number, duration));
        return self();
    }

    /**
     * Assert no records were received during the given duration
     *
     * @return self
     */
    public SELF awaitNoRecords(Duration duration) {
        try {
            awaitNextRecord(duration);
            throw new AssertionError("Received a record while expecting no records.");
        } catch (AssertionError e) {
            int size = subscriber.getItems().size();
            if (size != 0) {
                throw new AssertionError("Received " + size + " record(s) while expecting no records.");
            }
            return self();
        }
    }

    /**
     *
     * @return self
     */
    public SELF awaitCompletion() {
        subscriber.awaitCompletion();
        return self();
    }

    /**
     * Delegates to {@link AssertSubscriber#awaitCompletion(Duration)}
     *
     * @return self
     */
    public SELF awaitCompletion(Duration duration) {
        subscriber.awaitCompletion(duration);
        return self();
    }

    /**
     *
     * See {@link #awaitCompletion(BiConsumer, Duration)}
     *
     * @return self
     */
    public SELF awaitCompletion(BiConsumer<Throwable, Boolean> assertion) {
        return awaitCompletion(assertion, DEFAULT_TIMEOUT);
    }

    /**
     * Delegates to {@link AssertSubscriber#awaitCompletion()}
     *
     * @return self
     */
    public SELF awaitCompletion(BiConsumer<Throwable, Boolean> assertion, Duration duration) {
        try {
            subscriber.awaitCompletion(duration);
        } catch (AssertionError maybe) {
            subscriber.assertTerminated();
        }
        try {
            assertion.accept(subscriber.getFailure(), subscriber.isCancelled());
            return self();
        } catch (AssertionError e) {
            throw new AssertionError("Received a failure or cancellation event, but did not pass the validation: " + e, e);
        }
    }

    /**
     * Cancels subscription effectively stopping
     *
     * @return self
     */
    public SELF stop() {
        subscriber.cancel();
        subscriber.onComplete();
        return self();
    }

    @Override
    public void close() {
        stop();
    }

    public long firstOffset() {
        T firstRecord = getFirstRecord();
        if (firstRecord == null) {
            return -1;
        }
        return offset(firstRecord);
    }

    public long lastOffset() {
        T lastRecord = getLastRecord();
        if (lastRecord == null) {
            return -1;
        }
        return offset(lastRecord);
    }

    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public Map<TopicPartition, List<T>> byTopicPartition() {
        return stream().collect(Collectors.groupingBy(this::topicPartition));
    }

    public Map<TopicPartition, Long> latestOffsets() {
        return stream().collect(Collectors.toMap(this::topicPartition, this::offset, Math::max));
    }

    protected abstract long offset(T record);

    protected abstract TopicPartition topicPartition(T record);

}
