package io.smallrye.reactive.messaging.extension;

import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import io.smallrye.reactive.messaging.annotations.Broadcast;

public class EmitterConfiguration {
    public final String name;
    public final OnOverflow.Strategy overflowBufferStrategy;
    public final long overflowBufferSize;
    public final Boolean broadcast;
    public final int numberOfSubscriberBeforeConnecting;

    public EmitterConfiguration(String name, OnOverflow onOverflow, Broadcast broadcast) {
        this.name = name;

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
            this.broadcast = Boolean.FALSE;
            this.numberOfSubscriberBeforeConnecting = -1;
        }
    }
}
