package io.smallrye.reactive.messaging.kafka.base;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.Iterator;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

public class UnsatisfiedInstance<T> implements Instance<T> {

    private static final UnsatisfiedInstance<?> INSTANCE = new UnsatisfiedInstance<>();

    @SuppressWarnings("unchecked")
    public static <T> Instance<T> instance() {
        return (Instance<T>) INSTANCE;
    }

    private UnsatisfiedInstance() {
        // avoid direct instantiation
    }

    @Override
    public Instance<T> select(Annotation... qualifiers) {
        return instance();
    }

    @Override
    public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
        return instance();
    }

    @Override
    public <U extends T> Instance<U> select(TypeLiteral<U> subtype,
            Annotation... qualifiers) {
        return instance();
    }

    @Override
    public boolean isUnsatisfied() {
        return true;
    }

    @Override
    public boolean isAmbiguous() {
        return false;
    }

    @Override
    public void destroy(T instance) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public T get() {
        throw new UnsupportedOperationException();
    }
}
