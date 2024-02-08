package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;

/**
 * A polling stream that can be paused and resumed.
 *
 * @param <P> polled item type
 * @param <T> emitted item type
 */
public class PausablePollingStream<P, T> {

    private static final int STATE_NEW = 0; // no request yet -- we start polling on the first request
    private static final int STATE_POLLING = 1;
    private static final int STATE_PAUSED = 2;
    private static final int STATE_CANCELLED = 3;

    private final AtomicInteger state = new AtomicInteger(STATE_NEW);
    private final Queue<T> queue;
    private final ScheduledExecutorService pollerExecutor;
    private final String channel;
    private final int maxQueueSize;
    private final Uni<P> pollUni;
    private final UnicastProcessor<T> processor;
    private final Multi<T> stream;
    private final int halfMaxQueueSize;
    private final boolean pauseResumeEnabled;

    /**
     * Create a new polling stream.
     *
     * @param channel the channel name
     * @param pollUni the uni that polls the data
     * @param emitFunction the function that emits the polled data
     * @param pollerExecutor the executor that polls the data
     * @param maxQueueSize the maximum queue size
     * @param pauseResumeEnabled whether pause/resume is enabled
     */
    public PausablePollingStream(String channel, Uni<P> pollUni,
            BiConsumer<P, Flow.Processor<T, T>> emitFunction,
            ScheduledExecutorService pollerExecutor,
            int maxQueueSize,
            boolean pauseResumeEnabled) {
        this.channel = channel;
        this.maxQueueSize = maxQueueSize;
        this.halfMaxQueueSize = maxQueueSize / 2;
        this.pollerExecutor = pollerExecutor;
        this.pauseResumeEnabled = pauseResumeEnabled;
        this.queue = Queues.createSpscUnboundedArrayQueue(maxQueueSize);
        this.processor = UnicastProcessor.create(queue, null);
        this.pollUni = Uni.createFrom().deferred(() -> {
            if (state.get() != STATE_POLLING) {
                return Uni.createFrom().nullItem();
            }
            return pollUni.onItem().invoke(p -> emitFunction.accept(p, processor));
        });
        this.stream = processor.onRequest().invoke(n -> {
            if (state.compareAndSet(STATE_NEW, STATE_POLLING)) {
                poll();
            }
        });
    }

    public Multi<T> getStream() {
        return stream;
    }

    private void poll() {
        int state = this.state.get();
        if (state == STATE_CANCELLED || state == STATE_NEW) {
            return;
        }

        if (pauseResumeEnabled) {
            pauseResume();
        }

        pollUni.subscribe().with(messages -> {
            if (messages == null) {
                executeWithDelay(this::poll, Duration.ofMillis(2))
                        .subscribe().with(this::emptyConsumer, this::report);
            } else {
                runOnRequestThread(this::poll)
                        .subscribe().with(this::emptyConsumer, this::report);
            }
        }, this::report);
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
                processor.onError(fail);
                break;
            }
        }
    }

    private Uni<Void> runOnRequestThread(Runnable action) {
        return Uni.createFrom().voidItem().invoke(action).runSubscriptionOn(pollerExecutor);
    }

    private Uni<Void> executeWithDelay(Runnable action, Duration delay) {
        return Uni.createFrom().emitter(e -> {
            pollerExecutor.schedule(() -> {
                try {
                    action.run();
                } catch (Exception ex) {
                    e.fail(ex);
                    return;
                }
                e.complete(null);
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    private void pauseResume() {
        int size = queue.size();
        if (size >= maxQueueSize && state.compareAndSet(STATE_POLLING, STATE_PAUSED)) {
            log.pausingRequestingMessages(channel, size, maxQueueSize);
        } else if (size <= halfMaxQueueSize && state.compareAndSet(STATE_PAUSED, STATE_POLLING)) {
            log.resumingRequestingMessages(channel, size, halfMaxQueueSize);
        }
    }
}
