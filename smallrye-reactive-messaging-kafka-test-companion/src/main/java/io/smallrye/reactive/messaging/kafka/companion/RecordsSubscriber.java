package io.smallrye.reactive.messaging.kafka.companion;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.subscription.MultiSubscriber;

/**
 * A {@link io.smallrye.mutiny.Multi} {@link Subscriber} for testing purposes.
 *
 * @param <T> the type of the records
 */
class RecordsSubscriber<T, SELF extends RecordsSubscriber<T, SELF>> implements MultiSubscriber<T>, Iterable<T> {

    /**
     * The default timeout used by {@code await} method.
     */
    public static Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Latch waiting for the completion or failure event.
     */
    private final CountDownLatch terminal = new CountDownLatch(1);

    /**
     * The subscription received from upstream.
     */
    private final AtomicReference<Subscription> subscription = new AtomicReference<>();

    /**
     * The number of requested records.
     */
    private final AtomicLong requested = new AtomicLong();

    /**
     * The received records.
     */
    private final List<T> records = new CopyOnWriteArrayList<>();

    /**
     * The number of records received.
     */
    private final AtomicLong received = new AtomicLong();

    /**
     * The last record received.
     */
    private final AtomicReference<T> lastReceived = new AtomicReference<>();

    /**
     * The first record received.
     */
    private final AtomicReference<T> firstReceived = new AtomicReference<>();

    /**
     * The received failure.
     */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /**
     * Whether the multi completed successfully.
     */
    private final AtomicBoolean completed = new AtomicBoolean();

    /**
     * Whether the subscription has been cancelled.
     * This field is set to {@code true} when the subscriber calls {@code cancel} on the subscription.
     */
    private boolean cancelled;

    /**
     * Creates a new {@link RecordsSubscriber}.
     *
     * @param requested the number of initially requested records
     */
    public RecordsSubscriber(long requested) {
        this.requested.set(requested);
    }

    /**
     * Creates a new {@link RecordsSubscriber} with 0 requested records and no upfront cancellation.
     */
    public RecordsSubscriber() {
        this(0);
    }

    protected SELF self() {
        return (SELF) this;
    }

    /**
     * @return get the number of records received.
     */
    public long count() {
        return received.get();
    }

    /**
     * @return get the first record received, potentially {@code null} if no records have been received.
     */
    public T getFirstRecord() {
        return firstReceived.get();
    }

    /**
     * @return get the last record received, potentially {@code null} if no records have been received.
     */
    public T getLastRecord() {
        return lastReceived.get();
    }

    /**
     * The list of items that have been received.
     *
     * @return the list
     */
    public List<T> getRecords() {
        return records;
    }

    /**
     * Awaits for the next record.
     * If no record have been received before the default timeout, an {@link AssertionError} is thrown.
     * <p>
     * Note that it requests one record from the upstream.
     *
     * @return this {@link RecordsSubscriber}
     * @see #awaitNextRecords(int, int)
     */
    public SELF awaitNextRecord() {
        return awaitNextRecords(1);
    }

    /**
     * Awaits for the next record.
     * If no record have been received before the given timeout, an {@link AssertionError} is thrown.
     * <p>
     * Note that it requests one record from the upstream.
     *
     * @param duration the timeout, must not be {@code null}
     * @return this {@link RecordsSubscriber}
     * @see #awaitNextRecords(int, int)
     */
    public SELF awaitNextRecord(Duration duration) {
        return awaitNextRecords(1, 1, duration);
    }

    /**
     * Awaits for the next {@code number} records.
     * If not enough records have been received before the default timeout, an {@link AssertionError} is thrown.
     * <p>
     * This method requests {@code number} records the upstream.
     *
     * @param number the number of records to expect, must be neither 0 nor negative.
     * @return this {@link RecordsSubscriber}
     * @see #awaitNextRecords(int, int)
     */
    public SELF awaitNextRecords(int number) {
        return awaitNextRecords(number, DEFAULT_TIMEOUT);
    }

    /**
     * Awaits for the next {@code number} records.
     * If not enough records have been received before the default timeout, an {@link AssertionError} is thrown.
     *
     * @param number the number of records to expect, must neither be 0 nor negative.
     * @param request if not 0, the number of records to request upstream.
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitNextRecords(int number, int request) {
        return awaitNextRecords(number, request, DEFAULT_TIMEOUT);
    }

    /**
     * Awaits for the next {@code number} records.
     * If not enough records have been received before the given timeout, an {@link AssertionError} is thrown.
     * <p>
     * This method requests {@code number} records upstream.
     *
     * @param number the number of records to expect, must be neither 0 nor negative.
     * @param duration the timeout, must not be {@code null}
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitNextRecords(int number, Duration duration) {
        return awaitNextRecords(number, number, duration);
    }

    /**
     * Awaits for the next {@code number} records.
     * If not enough records have been received before the given timeout, an {@link AssertionError} is thrown.
     *
     * @param number the number of records to expect, must be neither 0 nor negative.
     * @param request if not 0, the number of records to request upstream.
     * @param duration the timeout, must not be {@code null}
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitNextRecords(int number, int request, Duration duration) {
        if (hasCompleted() || getFailure() != null) {
            if (hasCompleted()) {
                throw new AssertionError("Expecting a next records, but a completion event has already being received");
            } else {
                throw new AssertionError(
                        "Expecting a next records, but a failure event has already being received: " + getFailure());
            }
        }

        awaitNextRecordEvents(number, request, duration);

        return self();
    }

    /**
     * Awaits for the subscriber to receive {@code number} records in total (including the ones received after calling
     * this method).
     * If not enough records have been received before the default timeout, an {@link AssertionError} is thrown.
     * <p>
     * Unlike {@link #awaitNextRecords(int, int)}, this method does not request records from the upstream.
     *
     * @param number the number of records to expect, must be neither 0 nor negative.
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitRecords(int number) {
        return awaitRecords(number, DEFAULT_TIMEOUT);
    }

    /**
     * Awaits for the subscriber to receive {@code number} records in total (including the ones received after calling
     * this method).
     * If not enough records have been received before the given timeout, an {@link AssertionError} is thrown.
     * <p>
     * Unlike {@link #awaitNextRecords(int, int)}, this method does not requests records from the upstream.
     *
     * @param number the number of records to expect, must be neither 0 nor negative.
     * @param duration the timeout, must not be {@code null}
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitRecords(int number, Duration duration) {
        long receivedCount = count();
        if (receivedCount > number) {
            throw new AssertionError(
                    "Expected the number of records to be " + number + ", but it's already " + receivedCount);
        }

        if (isCancelled() || hasCompleted() || getFailure() != null) {
            if (receivedCount != number) {
                throw new AssertionError(
                        "Expected the number of records to be " + number + ", but received " + receivedCount
                                + " and we received a terminal event already");
            }
            return self();
        }

        awaitRecordEvents(number, duration);

        return self();
    }

    /**
     * Awaits for a completion event.
     * It waits at most {@link #DEFAULT_TIMEOUT}.
     * <p>
     * If the timeout expires, the check fails.
     *
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitCompletion() {
        return awaitCompletion(DEFAULT_TIMEOUT);
    }

    /**
     * Awaits for a completion event at most {@code duration}.
     * <p>
     * If the timeout expires, the check fails.
     *
     * @param duration the duration, must not be {@code null}
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitCompletion(Duration duration) {
        return awaitCompletion((t, c) -> {
        }, duration);
    }

    /**
     * Awaits for a completion event and validates it.
     * It waits at most {@code #DEFAULT_TIMEOUT}.
     * <p>
     * If the timeout expires without a terminal event (completion, failure or cancellation), the check fails immediately.
     * If a termination event is received in time, it is validated using the {@code assertion} consumer.
     * The code of the consumer is expected to throw an {@link AssertionError} to indicate that the failure didn't pass the
     * validation.
     *
     * @param assertion a check validating the received failure (if any). Must not be {@code null}
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitCompletion(BiConsumer<Throwable, Boolean> assertion) {
        return awaitCompletion(assertion, DEFAULT_TIMEOUT);
    }

    /**
     * Awaits for a completion event and validates it.
     * It waits at most {@code duration}.
     * <p>
     * If the timeout expires without a terminal event (completion, failure or cancellation), the check fails immediately.
     * If a termination event is received in time, it is validated using the {@code assertion} consumer.
     * The code of the consumer is expected to throw an {@link AssertionError} to indicate that the failure didn't pass the
     * validation.
     *
     * @param assertion a check validating the received failure or cancellation (if any). Must not be {@code null}
     * @param duration the max duration to wait, must not be {@code null}
     * @return this {@link RecordsSubscriber}
     */
    public SELF awaitCompletion(BiConsumer<Throwable, Boolean> assertion, Duration duration) {
        try {
            awaitEvent(terminal, duration);
        } catch (TimeoutException e) {
            throw new AssertionError(
                    "No completion event received in the last " + duration.toMillis() + " ms");
        }

        final Throwable throwable = failure.get();
        try {
            assertion.accept(throwable, cancelled);
            return self();
        } catch (AssertionError e) {
            throw new AssertionError("Received a failure or cancellation event, but did not pass the validation: " + e, e);
        }
    }

    private void awaitEvent(CountDownLatch latch, Duration duration) throws TimeoutException {
        // Are we already done?
        if (latch.getCount() == 0) {
            return;
        }
        try {
            if (!latch.await(duration.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private final List<EventListener> eventListeners = new CopyOnWriteArrayList<>();

    private void awaitNextRecordEvents(int number, int request, Duration duration) {
        long currentCount = count();
        RecordTask task = new RecordTask(currentCount + number, self());
        if (request > 0) {
            request(request);
        }
        try {
            task.future().get(duration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // Terminal event received
            long received = count() - currentCount;
            if (isCancelled()) {
                throw new AssertionError(
                        "Expected " + number + " records, but received a cancellation event while waiting. Only "
                                + received
                                + " record(s) have been received.");
            } else if (hasCompleted()) {
                throw new AssertionError(
                        "Expected " + number + " records, but received a completion event while waiting. Only " + received
                                + " record(s) have been received.");
            } else {
                throw new AssertionError(
                        "Expected " + number + " records, but received a failure event while waiting: " + getFailure()
                                + ". Only "
                                + received + " record(s) have been received.");
            }
        } catch (TimeoutException e) {
            // Timeout
            long received = count() - currentCount;
            throw new AssertionError(
                    "Expected " + number + " records in " + duration.toMillis() + " ms, but only received " + received
                            + " records.");
        }
    }

    private void awaitRecordEvents(int expected, Duration duration) {
        RecordTask task = new RecordTask(expected, self());
        try {
            task.future().get(duration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            long receivedCount = count();
            if (expected == receivedCount) {
                return;
            }
            // Terminal event received
            if (isCancelled()) {
                throw new AssertionError(
                        "Expected " + expected + " records, but received a cancellation event while waiting. Only "
                                + receivedCount
                                + " records have been received.");
            } else if (hasCompleted()) {
                throw new AssertionError(
                        "Expected " + expected + " records, but received a completion event while waiting. Only " +
                                receivedCount
                                + " records have been received.");
            } else if (getFailure() != null) {
                throw new AssertionError(
                        "Expected " + expected + " records, but received a failure event while waiting: " + getFailure()
                                + ". Only " + receivedCount + " records have been received.");
            } else {
                throw new AssertionError(
                        "Expected " + expected + " records.  Only " + receivedCount + " records have been received.");
            }
        } catch (TimeoutException e) {
            long receivedCount = count();
            // Timeout, but verify we didn't get event while timing out.
            if (receivedCount >= expected) {
                return;
            }
            throw new AssertionError(
                    "Expected " + expected + " records.  Only " + receivedCount + " records have been received.");
        }
    }

    /**
     * Cancel the subscription.
     *
     * @return this {@link RecordsSubscriber}
     */
    public SELF cancel() {
        if (subscription.get() != null) {
            subscription.get().cancel();
        }
        cancelled = true;
        terminal.countDown();
        Event ev = new Event(null, null, false, true);
        eventListeners.forEach(l -> l.accept(ev));
        return self();
    }

    /**
     * Request records.
     *
     * @param req the number of records to request.
     * @return this {@link RecordsSubscriber}
     */
    private SELF request(long req) {
        requested.addAndGet(req);
        if (subscription.get() != null) {
            subscription.get().request(req);
        }
        return self();
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (subscription.compareAndSet(null, s)) {
            if (requested.get() > 0) {
                s.request(requested.get());
            }
        }
    }

    @Override
    public synchronized void onItem(T t) {
        long newCount = received.incrementAndGet();
        if (newCount == 1L) {
            firstReceived.set(t);
        }
        this.received(t);
        lastReceived.set(t);
        Event ev = new Event(newCount, null, false, false);
        eventListeners.forEach(l -> l.accept(ev));
    }

    public void received(T t) {
        records.add(t);
    }

    @Override
    public void onFailure(Throwable t) {
        failure.set(t);
        terminal.countDown();
        Event ev = new Event(null, t, false, false);
        eventListeners.forEach(l -> l.accept(ev));
    }

    @Override
    public void onCompletion() {
        completed.set(true);
        terminal.countDown();
        Event ev = new Event(null, null, true, false);
        eventListeners.forEach(l -> l.accept(ev));
    }

    @Override
    public Iterator<T> iterator() {
        return getRecords().iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return getRecords().spliterator();
    }

    /**
     * The reported failure, if any.
     *
     * @return the failure or {@code null}
     */
    public Throwable getFailure() {
        return failure.get();
    }

    /**
     * Check whether the subscription has been cancelled or not.
     *
     * @return a boolean
     */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * Check whether the multi has completed.
     *
     * @return a boolean
     */
    public boolean hasCompleted() {
        return completed.get();
    }

    public boolean hasSubscribed() {
        return subscription.get() != null;
    }

    protected void registerListener(EventListener listener) {
        eventListeners.add(listener);
    }

    protected void unregisterListener(EventListener listener) {
        eventListeners.remove(listener);
    }

    private interface EventListener extends Consumer<Event> {
    }

    private static class Event {

        private final Long recordCount;
        private final Throwable failure;
        private final boolean completion;
        private final boolean cancellation;

        private Event(Long recordCount, Throwable failure, boolean completion, boolean cancellation) {
            this.recordCount = recordCount;
            this.failure = failure;
            this.completion = completion;
            this.cancellation = cancellation;
        }

        public Long recordCount() {
            return recordCount;
        }

        public boolean isRecord() {
            return recordCount != null;
        }

        public boolean isCancellation() {
            return cancellation;
        }

        public boolean isFailure() {
            return failure != null;
        }

        public boolean isCompletion() {
            return completion;
        }
    }

    private class RecordTask {

        private final long targetCount;
        private final SELF subscriber;
        private final long startCount;

        public RecordTask(long targetCount, SELF subscriber) {
            this.targetCount = targetCount;
            this.subscriber = subscriber;
            this.startCount = subscriber.count();
        }

        public CompletableFuture<Void> future() {
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (startCount >= targetCount) {
                future.complete(null);
                return future;
            }
            EventListener listener = event -> {
                if (event.recordCount() != null && event.recordCount >= targetCount) {
                    future.complete(null);
                } else if (event.isCancellation() || event.isFailure() || event.isCompletion()) {
                    future.completeExceptionally(
                            new NoSuchElementException("Received a terminal event while waiting for records"));
                }
                // Else wait for timeout or next event.
            };
            subscriber.registerListener(listener);
            return future.whenComplete((x, f) -> subscriber.unregisterListener(listener));
        }
    }

}
