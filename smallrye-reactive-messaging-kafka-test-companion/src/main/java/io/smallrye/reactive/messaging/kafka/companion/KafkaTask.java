package io.smallrye.reactive.messaging.kafka.companion;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Multi;

/**
 * Abstract task for consuming or producing Kafka records provided by the given {@link Multi}
 * <p>
 * This class leverages {@link RecordsSubscriber} to subscribe itself to the given multi.
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
     * The {@link RecordsSubscriber} for controlled subscription
     */
    private final RecordsSubscriber<T, ?> subscriber;

    /**
     * Create a new {@link KafkaTask}
     *
     * @param multi the multi to subscribe to
     */
    public KafkaTask(Multi<T> multi) {
        super();
        this.multi = multi;
        this.subscriber = new RecordsSubscriber<>(Long.MAX_VALUE);
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
        return subscriber.count();
    }

    /**
     * @return get the first record received, potentially {@code null} if no records have been received.
     */
    public T getFirstRecord() {
        return subscriber.getFirstRecord();
    }

    /**
     * @return get the last record received, potentially {@code null} if no records have been received.
     */
    public T getLastRecord() {
        return subscriber.getLastRecord();
    }

    /**
     * The list of items that have been received.
     *
     * @return the list
     */
    public List<T> getRecords() {
        return subscriber.getRecords();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitNextRecord()}
     *
     * @return self
     */
    public SELF awaitNextRecord() {
        subscriber.awaitNextRecord();
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitNextRecord(Duration)}
     *
     * @return self
     */
    public SELF awaitNextRecord(Duration duration) {
        subscriber.awaitNextRecord(duration);
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitNextRecords(int)}
     *
     * @return self
     */
    public SELF awaitNextRecords(int number) {
        subscriber.awaitNextRecords(number);
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitNextRecords(int, Duration)}
     *
     * @return self
     */
    public SELF awaitNextRecords(int number, Duration duration) {
        subscriber.awaitNextRecords(number, duration);
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitRecords(int)}
     *
     * @return self
     */
    public SELF awaitRecords(int number) {
        subscriber.awaitRecords(number);
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitRecords(int, Duration)}
     *
     * @return self
     */
    public SELF awaitRecords(int number, Duration duration) {
        subscriber.awaitRecords(number, duration);
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitCompletion()}
     *
     * @return self
     */
    public SELF awaitCompletion() {
        subscriber.awaitCompletion();
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitCompletion(Duration)}
     *
     * @return self
     */
    public SELF awaitCompletion(Duration duration) {
        subscriber.awaitCompletion(duration);
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitCompletion(BiConsumer)}
     *
     * @return self
     */
    public SELF awaitCompletion(BiConsumer<Throwable, Boolean> assertion) {
        subscriber.awaitCompletion(assertion);
        return self();
    }

    /**
     * Delegates to {@link RecordsSubscriber#awaitCompletion(BiConsumer, Duration)}
     *
     * @return self
     */
    public SELF awaitCompletion(BiConsumer<Throwable, Boolean> assertion, Duration duration) {
        subscriber.awaitCompletion(assertion, duration);
        return self();
    }

    /**
     * Cancels subscription effectively stopping
     *
     * @return self
     */
    public SELF stop() {
        subscriber.cancel();
        return self();
    }

    @Override
    public void close() {
        stop();
    }

    public long firstOffset() {
        T firstRecord = subscriber.getFirstRecord();
        if (firstRecord == null) {
            return -1;
        }
        return offset(firstRecord);
    }

    public long lastOffset() {
        T lastRecord = subscriber.getLastRecord();
        if (lastRecord == null) {
            return -1;
        }
        return offset(lastRecord);
    }

    public Map<TopicPartition, List<T>> byTopicPartition() {
        return subscriber.getRecords().stream().collect(Collectors.groupingBy(this::topicPartition));
    }

    protected abstract long offset(T record);

    protected abstract TopicPartition topicPartition(T record);

}
