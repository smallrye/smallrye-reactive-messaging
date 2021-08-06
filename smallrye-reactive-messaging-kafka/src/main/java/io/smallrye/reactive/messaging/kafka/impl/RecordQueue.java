package io.smallrye.reactive.messaging.kafka.impl;

import java.util.ArrayDeque;
import java.util.Collection;

/**
 * Stores the records coming from Kafka.
 * Only a few operations are supported: {@link #addAll(Iterable)}, {@link #clear()}, {@link #size()} and {@link #poll()}.
 * <p>
 * The access is guarded by the monitor lock.
 */
public class RecordQueue<T> extends ArrayDeque<T> {

    public RecordQueue(int capacityHint) {
        super(capacityHint);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    public void addAll(Iterable<T> iterable) {
        synchronized (this) {
            for (T record : iterable) {
                super.offer(record);
            }
        }
    }

    @Override
    public boolean add(T item) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(T item) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T poll() {
        T record;
        synchronized (this) {
            record = super.poll();
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
        synchronized (this) {
            return super.size();
        }
    }

    @Override
    public void clear() {
        synchronized (this) {
            super.clear();
        }
    }
}
