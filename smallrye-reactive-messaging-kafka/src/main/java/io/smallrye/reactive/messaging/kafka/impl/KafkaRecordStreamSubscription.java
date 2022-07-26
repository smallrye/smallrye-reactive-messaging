package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.vertx.core.Context;

/**
 * A {@link Flow.Subscription} which, on {@link #request(long)}, polls {@link ConsumerRecords} from the given consumer client
 * and emits records downstream.
 * <p>
 * It uses an internal queue to store records received but not yet emitted downstream.
 * The given enqueue function can flatten the polled {@link ConsumerRecords} into individual records or enqueue it as-is.
 *
 * @param <K> type of incoming record key
 * @param <V> type of incoming record payload
 * @param <T> type of the items to emit downstream
 */
public class KafkaRecordStreamSubscription<K, V, T> implements Flow.Subscription {
    private static final int STATE_NEW = 0; // no request yet -- we start polling on the first request
    private static final int STATE_POLLING = 1;
    private static final int STATE_PAUSED = 2;
    private static final int STATE_CANCELLED = 3;

    private final ReactiveKafkaConsumer<K, V> client;
    private final String clientId;
    private volatile MultiSubscriber<? super T> downstream;
    private final Context context;
    private final boolean pauseResumeEnabled;

    /**
     * Current state: new (no request yet), polling, paused, cancelled
     */
    private final AtomicInteger state = new AtomicInteger(STATE_NEW);

    private final AtomicInteger wip = new AtomicInteger();
    /**
     * Stores the current downstream demands.
     */
    private final AtomicLong requested = new AtomicLong();

    /**
     * The polling uni to avoid re-assembling a Uni everytime.
     */
    private final Uni<ConsumerRecords<K, V>> pollUni;

    private final String channel;
    private final int maxQueueSize;
    private final int halfMaxQueueSize;
    private final RecordQueue<T> queue;
    private final long retries;

    public KafkaRecordStreamSubscription(
            ReactiveKafkaConsumer<K, V> client,
            KafkaConnectorIncomingConfiguration config,
            MultiSubscriber<? super T> subscriber,
            Context context,
            int maxPollRecords,
            BiConsumer<ConsumerRecords<K, V>, RecordQueue<T>> enqueueFunction) {
        this.client = client;
        this.clientId = client.get(ConsumerConfig.CLIENT_ID_CONFIG);
        this.channel = config.getChannel();
        this.pauseResumeEnabled = config.getPauseIfNoRequests();
        this.downstream = subscriber;
        this.context = context;
        this.maxQueueSize = maxPollRecords * config.getMaxQueueSizeFactor();
        this.halfMaxQueueSize = (maxPollRecords == 1 ? 0 : maxPollRecords);
        // we can exceed maxQueueSize by at most 1 maxPollRecords
        this.queue = new RecordQueue<>(maxQueueSize + maxPollRecords);
        this.retries = config.getRetryAttempts() == -1 ? Long.MAX_VALUE : config.getRetryAttempts();
        this.pollUni = client.poll()
                .onItem().transform(cr -> {
                    if (cr.isEmpty()) {
                        return null;
                    }
                    if (log.isTraceEnabled()) {
                        log.tracef("Adding %s messages to the queue", cr.count());
                    }
                    enqueueFunction.accept(cr, queue);
                    return cr;
                })
                .plug(m -> {
                    if (config.getRetry()) {
                        int maxWait = config.getRetryMaxWait();
                        return m.onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                                .atMost(retries);
                    }
                    return m;
                });
    }

    @Override
    public void request(long n) {
        if (n > 0) {
            boolean cancelled = state.get() == STATE_CANCELLED;
            if (!cancelled) {
                Subscriptions.add(requested, n);
                if (state.compareAndSet(STATE_NEW, STATE_POLLING)) {
                    poll();
                } else {
                    dispatch();
                }
            }
        } else {
            throw new IllegalArgumentException("Invalid request");
        }

    }

    private void poll() {
        int state = this.state.get();
        if (state == STATE_CANCELLED || state == STATE_NEW || client.isClosed()) {
            return;
        }

        if (pauseResumeEnabled) {
            pauseResume();
        }

        pollUni.subscribe().with(cr -> {
            if (cr == null) {
                client.executeWithDelay(this::poll, Duration.ofMillis(2))
                        .subscribe().with(this::emptyConsumer, this::report);
            } else {
                dispatch();
                client.runOnPollingThread(c -> {
                    poll();
                }).subscribe().with(this::emptyConsumer, this::report);
            }
        }, this::report);
    }

    private void pauseResume() {
        int size = queue.size();
        if (size >= maxQueueSize && state.compareAndSet(STATE_POLLING, STATE_PAUSED)) {
            log.pausingChannel(channel, clientId, size, maxQueueSize);
            client.pause()
                    .subscribe().with(this::emptyConsumer, this::report);
        } else if (size <= halfMaxQueueSize && state.compareAndSet(STATE_PAUSED, STATE_POLLING)) {
            log.resumingChannel(channel, clientId, size, halfMaxQueueSize);
            client.resume()
                    .subscribe().with(this::emptyConsumer, this::report);
        }
    }

    private <I> void emptyConsumer(I ignored) {
    }

    private void report(Throwable fail) {
        while (true) {
            int state = this.state.get();
            if (state == STATE_CANCELLED) {
                break;
            }
            if (this.state.compareAndSet(state, STATE_CANCELLED)) {
                downstream.onFailure(fail);
                break;
            }
        }
    }

    void dispatch() {
        if (wip.getAndIncrement() != 0) {
            return;
        }
        context.runOnContext(ignored -> run());
    }

    private void run() {
        int missed = 1;
        final RecordQueue<T> q = queue;
        long emitted = 0;
        long requests = requested.get();
        for (;;) {
            if (isCancelled()) {
                return;
            }

            while (emitted != requests) {
                T item = q.poll();

                if (item == null || isCancelled()) {
                    break;
                }

                downstream.onItem(item);
                emitted++;
            }

            requests = requested.addAndGet(-emitted);
            emitted = 0;

            int w = wip.get();
            if (missed == w) {
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            } else {
                missed = w;
            }
        }
    }

    @Override
    public void cancel() {
        while (true) {
            int state = this.state.get();
            if (state == STATE_CANCELLED) {
                break;
            }
            if (this.state.compareAndSet(state, STATE_CANCELLED)) {
                if (wip.getAndIncrement() == 0) {
                    // nothing was currently dispatched, clearing the queue.
                    client.close();
                    queue.clear();
                    downstream = null;
                }
                break;
            }
        }
    }

    boolean isCancelled() {
        if (state.get() == STATE_CANCELLED) {
            queue.clear();
            client.close();
            downstream = null;
            return true;
        }
        return false;
    }

    /**
     * Uses a mapping function to replaces all items in the queue.
     * If the mapping function returns null item will simply be removed
     * from the queue.
     *
     * Order is preserved.
     *
     * @param mapFunction
     */
    void rewriteQueue(UnaryOperator<T> mapFunction) {
        ArrayDeque<T> replacementQueue = new ArrayDeque<>();
        synchronized (queue) {
            queue
                    .stream()
                    .map(mapFunction)
                    .filter(Objects::nonNull)
                    .forEach(replacementQueue::offer);

            queue.clear();
            queue.addAll((Iterable<T>) replacementQueue);
        }
    }
}
