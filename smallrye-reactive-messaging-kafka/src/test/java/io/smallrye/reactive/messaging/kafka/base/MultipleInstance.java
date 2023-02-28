package io.smallrye.reactive.messaging.kafka.base;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import io.smallrye.common.annotation.Identifier;

public class MultipleInstance<T> implements Instance<T> {

    private final Map<String, T> instances;

    public MultipleInstance(T... instances) {
        this.instances = new HashMap<>();
        for (T instance : instances) {
            Identifier identifier = instance.getClass().getAnnotation(Identifier.class);
            this.instances.put(identifier.value(), instance);
        }
    }

    @Override
    public Instance<T> select(Annotation... qualifiers) {
        if (qualifiers.length == 0) {
            return this;
        }
        if (qualifiers.length == 1 && qualifiers[0] instanceof Identifier) {
            String name = ((Identifier) qualifiers[0]).value();
            if (instances.containsKey(name)) {
                return new SingletonInstance<>(name, instances.get(name));
            }
        }
        return UnsatisfiedInstance.instance();
    }

    @Override
    public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U extends T> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
        throw new UnsupportedOperationException();
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
    public Handle<T> getHandle() {
        return null;
    }

    @Override
    public Iterable<? extends Handle<T>> handles() {
        return null;
    }

    @Override
    public Iterator<T> iterator() {
        return instances.values().iterator();
    }

    @Override
    public T get() {
        return instances.values().stream().findFirst().orElse(null);
    }
}
