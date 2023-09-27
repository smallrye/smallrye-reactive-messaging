package io.smallrye.reactive.messaging.kafka.impl;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

/**
 * Stores the records coming from Kafka.
 * Only a few operations are supported: {@link #offer(Object)}, {@link #addAll(Iterable)}, {@link #clear()},
 * {@link #size()} and {@link #poll()}.
 * <p>
 * The access is guarded by the monitor lock.
 */
public class RecordQueue<T> extends ArrayDeque<T> {

    private final ReentrantLock lock = new ReentrantLock();

    public RecordQueue(int capacityHint) {
        super(capacityHint);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    public void addAll(Iterable<T> iterable) {
        lock.lock();
        try {
            for (T record : iterable) {
                super.offer(record);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean add(T item) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(T item) {
        lock.lock();
        try {
            return super.offer(item);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T poll() {
        T record;
        lock.lock();
        try {
            record = super.poll();
        } finally {
            lock.unlock();
        }
        return record;
    }

    @Override
    public T peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return super.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            super.clear();
        } finally {
            lock.unlock();
        }
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
        lock.lock();
        try {
            ArrayDeque<T> replacementQueue = new ArrayDeque<>();
            this
                    .stream()
                    .map(mapFunction)
                    .filter(Objects::nonNull)
                    .forEach(replacementQueue::offer);

            this.clear();
            this.addAll((Iterable<T>) replacementQueue);
        } finally {
            lock.unlock();
        }
    }
}
