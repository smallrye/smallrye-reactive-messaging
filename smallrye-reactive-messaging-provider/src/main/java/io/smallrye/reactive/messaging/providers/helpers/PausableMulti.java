package io.smallrye.reactive.messaging.providers.helpers;

import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.MultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.PausableChannelConfiguration;

public class PausableMulti<T> extends MultiOperator<T, T> implements PausableChannel {

    private volatile PausableProcessor processor;

    private final AtomicBoolean paused;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final boolean lateSubscription;
    private final boolean bufferAlreadyRequested;

    public PausableMulti(Multi<T> upstream, PausableChannelConfiguration configuration) {
        super(upstream);
        this.paused = new AtomicBoolean(configuration.initiallyPaused());
        this.lateSubscription = configuration.lateSubscription();
        this.bufferAlreadyRequested = configuration.bufferAlreadyRequested();
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> subscriber) {
        processor = new PausableProcessor(subscriber);
        if (!lateSubscription || !paused.get()) { // if late subscription is disabled, we can subscribe now.
            subscribed.set(true);
            upstream().subscribe(processor);
        }
    }

    @Override
    public boolean isPaused() {
        return paused.get();
    }

    @Override
    public void pause() {
        paused.set(true);
    }

    @Override
    public void resume() {
        if (paused.compareAndSet(true, false)) {
            PausableProcessor p = processor;
            if (p != null) {
                if (lateSubscription && subscribed.compareAndSet(false, true)) {
                    upstream().subscribe(p);
                }
                p.resume();
            }
        }
    }

    @Override
    public int bufferSize() {
        PausableProcessor p = processor;
        if (p != null) {
            return p.queueSize();
        }
        return 0;
    }

    @Override
    public boolean clearBuffer() {
        if (paused.get()) {
            PausableProcessor p = processor;
            if (p != null) {
                p.clearQueue();
                return true;
            }
        }
        return false;
    }

    private class PausableProcessor extends MultiOperatorProcessor<T, T> {

        private final AtomicLong demand = new AtomicLong();
        private final Queue<T> queue;

        PausableProcessor(MultiSubscriber<? super T> downstream) {
            super(downstream);
            this.queue = Queues.<T> unbounded(Infrastructure.getMultiOverflowDefaultBufferSize()).get();
        }

        void resume() {
            Flow.Subscription subscription = getUpstreamSubscription();
            if (subscription == Subscriptions.CANCELLED) {
                return;
            }
            if (bufferAlreadyRequested) {
                drainQueue();
            }
            long currentDemand = demand.get();
            if (currentDemand > 0) {
                Subscriptions.produced(demand, currentDemand);
                subscription.request(currentDemand);
            }
        }

        void drainQueue() {
            Queue<T> qe = queue;
            while (!qe.isEmpty() && !paused.get()) {
                T item = qe.poll();
                if (item == null) {
                    break;
                }
                downstream.onItem(item);
            }
        }

        void clearQueue() {
            queue.clear();
        }

        int queueSize() {
            return queue.size();
        }

        @Override
        public void onItem(T item) {
            if (bufferAlreadyRequested && paused.get()) {
                queue.offer(item);
            } else {
                super.onItem(item);
            }
        }

        @Override
        public void request(long numberOfItems) {
            if (numberOfItems <= 0) {
                onFailure(Subscriptions.getInvalidRequestException());
                return;
            }
            Flow.Subscription subscription = getUpstreamSubscription();
            if (subscription == Subscriptions.CANCELLED) {
                return;
            }
            try {
                Subscriptions.add(demand, numberOfItems);
                if (paused.get()) {
                    return;
                }
                long currentDemand = demand.get();
                if (currentDemand > 0) {
                    Subscriptions.produced(demand, currentDemand);
                    subscription.request(currentDemand);
                }
            } catch (Throwable failure) {
                onFailure(failure);
            }
        }

        @Override
        public void cancel() {
            processor = null;
            super.cancel();
        }
    }
}
