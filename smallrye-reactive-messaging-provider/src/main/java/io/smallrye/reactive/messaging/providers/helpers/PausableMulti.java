package io.smallrye.reactive.messaging.providers.helpers;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.operators.MultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.PausableChannel;

public class PausableMulti<T> extends MultiOperator<T, T> implements PausableChannel {

    private volatile boolean paused;
    volatile PausableProcessor processor;
    private final ReentrantLock lock = new ReentrantLock();

    public PausableMulti(Multi<T> upstream, boolean paused) {
        super(upstream);
        this.paused = paused;
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> subscriber) {
        processor = new PausableProcessor(subscriber);
        upstream().subscribe(processor);
    }

    @Override
    public boolean isPaused() {
        return paused;
    }

    @Override
    public void pause() {
        lock.lock();
        try {
            paused = true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void resume() {
        lock.lock();
        try {
            if (paused) {
                PausableProcessor p = processor;
                if (p != null) {
                    paused = false;
                    p.resume();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private class PausableProcessor extends MultiOperatorProcessor<T, T> {

        private final AtomicLong demand = new AtomicLong();

        PausableProcessor(MultiSubscriber<? super T> downstream) {
            super(downstream);
        }

        void resume() {
            Flow.Subscription subscription = getUpstreamSubscription();
            if (subscription == Subscriptions.CANCELLED) {
                return;
            }
            long currentDemand = demand.get();
            if (currentDemand > 0) {
                Subscriptions.produced(demand, currentDemand);
                subscription.request(currentDemand);
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
                long currentDemand = demand.get();
                if (paused) {
                    return;
                }
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
