package io.smallrye.reactive.messaging.providers;

import java.util.Objects;

import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;

public class DefaultEmitterConfiguration implements EmitterConfiguration {

    private String name;
    private EmitterFactoryFor emitterType;
    private OnOverflow.Strategy overflowBufferStrategy;
    private long overflowBufferSize;
    private boolean broadcast;
    private int numberOfSubscriberBeforeConnecting;

    public DefaultEmitterConfiguration() {
    }

    public DefaultEmitterConfiguration(String name, EmitterFactoryFor emitterType, OnOverflow onOverflow, Broadcast broadcast) {
        this.name = name;
        this.emitterType = emitterType;

        if (onOverflow != null) {
            this.overflowBufferStrategy = onOverflow.value();
            this.overflowBufferSize = onOverflow.bufferSize();
        } else {
            this.overflowBufferStrategy = null;
            this.overflowBufferSize = -1;
        }

        if (broadcast != null) {
            this.broadcast = Boolean.TRUE;
            this.numberOfSubscriberBeforeConnecting = broadcast.value();
        } else {
            this.numberOfSubscriberBeforeConnecting = -1;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public EmitterFactoryFor emitterType() {
        return emitterType;
    }

    @Override
    public OnOverflow.Strategy overflowBufferStrategy() {
        return overflowBufferStrategy;
    }

    @Override
    public long overflowBufferSize() {
        return overflowBufferSize;
    }

    @Override
    public boolean broadcast() {
        return broadcast;
    }

    @Override
    public int numberOfSubscriberBeforeConnecting() {
        return numberOfSubscriberBeforeConnecting;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DefaultEmitterConfiguration that = (DefaultEmitterConfiguration) o;
        return overflowBufferSize == that.overflowBufferSize
                && broadcast == that.broadcast
                && numberOfSubscriberBeforeConnecting == that.numberOfSubscriberBeforeConnecting
                && Objects.equals(name, that.name)
                && Objects.equals(emitterType, that.emitterType)
                && overflowBufferStrategy == that.overflowBufferStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, emitterType, overflowBufferStrategy, overflowBufferSize, broadcast,
                numberOfSubscriberBeforeConnecting);
    }

    @Override
    public String toString() {
        return "DefaultEmitterConfiguration{" +
                "name='" + name + '\'' +
                ", emitterType=" + emitterType +
                ", overflowBufferStrategy=" + overflowBufferStrategy +
                ", overflowBufferSize=" + overflowBufferSize +
                ", broadcast=" + broadcast +
                ", numberOfSubscriberBeforeConnecting=" + numberOfSubscriberBeforeConnecting +
                '}';
    }
}
