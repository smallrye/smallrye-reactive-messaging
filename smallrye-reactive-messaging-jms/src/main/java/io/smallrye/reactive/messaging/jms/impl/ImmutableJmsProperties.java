package io.smallrye.reactive.messaging.jms.impl;

import static io.smallrye.reactive.messaging.jms.impl.Wrap.wrap;

import java.util.Enumeration;

import jakarta.jms.Message;

import io.smallrye.reactive.messaging.jms.JmsProperties;

public final class ImmutableJmsProperties implements JmsProperties {
    private final Message delegate;

    public ImmutableJmsProperties(Message message) {
        this.delegate = message;
    }

    @Override
    public boolean propertyExists(String name) {
        return wrap(() -> delegate.propertyExists(name));
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return wrap(() -> delegate.getBooleanProperty(name));
    }

    @Override
    public byte getByteProperty(String name) {
        return wrap(() -> delegate.getByteProperty(name));
    }

    @Override
    public short getShortProperty(String name) {
        return wrap(() -> delegate.getShortProperty(name));
    }

    @Override
    public int getIntProperty(String name) {
        return wrap(() -> delegate.getIntProperty(name));
    }

    @Override
    public long getLongProperty(String name) {
        return wrap(() -> delegate.getLongProperty(name));
    }

    @Override
    public float getFloatProperty(String name) {
        return wrap(() -> delegate.getFloatProperty(name));
    }

    @Override
    public double getDoubleProperty(String name) {
        return wrap(() -> delegate.getDoubleProperty(name));
    }

    @Override
    public String getStringProperty(String name) {
        return wrap(() -> delegate.getStringProperty(name));
    }

    @Override
    public Object getObjectProperty(String name) {
        return wrap(() -> delegate.getObjectProperty(name));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Enumeration<String> getPropertyNames() {
        return wrap(delegate::getPropertyNames);
    }
}
