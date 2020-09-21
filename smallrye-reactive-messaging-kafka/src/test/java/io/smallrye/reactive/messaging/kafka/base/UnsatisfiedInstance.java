package io.smallrye.reactive.messaging.kafka.base;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.Iterator;

import javax.enterprise.inject.Instance;
import javax.enterprise.util.TypeLiteral;

public class UnsatisfiedInstance<T> implements Instance<T> {

    public static <T> UnsatisfiedInstance<T> instance() {
        return new UnsatisfiedInstance<>();
    }

    private UnsatisfiedInstance() {
        // avoid direct instantiation
    }

    @Override
    public Instance<T> select(Annotation... qualifiers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U extends T> Instance<U> select(TypeLiteral<U> subtype,
            Annotation... qualifiers) {
        throw new UnsupportedOperationException();
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
