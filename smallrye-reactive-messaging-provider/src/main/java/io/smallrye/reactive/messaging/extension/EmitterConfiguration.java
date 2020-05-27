package io.smallrye.reactive.messaging.extension;

import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import io.smallrye.reactive.messaging.annotations.Broadcast;

public class EmitterConfiguration {
    private final String name;
    private OnOverflow.Strategy overflowBufferStrategy = null;
    private long overflowBufferSize = -1;
    private Boolean broadcast = Boolean.FALSE;
    private int numberOfSubscriberBeforeConnecting = -1;

    public EmitterConfiguration(String name, OnOverflow onOverflow, Broadcast broadcast) {
        this.name = name;

        if (onOverflow != null) {
            this.overflowBufferStrategy = onOverflow.value();
            this.overflowBufferSize = onOverflow.bufferSize();
        }

        if (broadcast != null) {
            this.broadcast = Boolean.TRUE;
            this.numberOfSubscriberBeforeConnecting = broadcast.value();
        }
    }

    public String getName() {
        return this.name;
    }

    public OnOverflow.Strategy getOverflowBufferStrategy() {
        return this.overflowBufferStrategy;
    }

    public long getOverflowBufferSize() {
        return this.overflowBufferSize;
    }

    public Boolean isBroadcast() {
        return broadcast;
    }

    public int getNumberOfSubscriberBeforeConnecting() {
        return this.numberOfSubscriberBeforeConnecting;
    }
}
