package io.smallrye.reactive.messaging.kafka.base;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.Iterator;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import io.smallrye.common.annotation.Identifier;

public class SingletonInstance<T> implements Instance<T> {

    private final T instance;
    private final String name;

    public SingletonInstance(String name, T instance) {
        this.name = name;
        this.instance = instance;
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
        return false;
    }

    @Override
    public void destroy(T instance) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        return Collections.singleton(instance).iterator();
    }

    @Override
    public T get() {
        return instance;
    }
}
