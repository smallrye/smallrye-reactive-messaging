package io.smallrye.reactive.messaging.kafka.base;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Iterator;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import io.smallrye.common.annotation.Identifier;

public class DoubleInstance<T> implements Instance<T> {

    private final T instance1;
    private final T instance2;
    private final String name;

    public DoubleInstance(String name, T instance1, T instance2) {
        this.name = name;
        this.instance1 = instance1;
        this.instance2 = instance2;
    }

    @Override
    public Instance<T> select(Annotation... qualifiers) {
        if (qualifiers.length == 0) {
            return this;
        }
        if (qualifiers.length == 1 && qualifiers[0] instanceof Identifier) {
            if (((Identifier) qualifiers[0]).value().equalsIgnoreCase(name)) {
                return this;
            }
        }
        return UnsatisfiedInstance.instance();

    }

    @Override
    public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public <U extends T> Instance<U> select(TypeLiteral<U> subtype,
            Annotation... qualifiers) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean isUnsatisfied() {
        return false;
    }

    @Override
    public boolean isAmbiguous() {
        return true;
    }

    @Override
    public void destroy(T instance) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        return Arrays.asList(instance1, instance2).iterator();
    }

    @Override
    public T get() {
        return instance1;
    }
}
