package io.smallrye.reactive.messaging.extension;

import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import io.smallrye.reactive.messaging.annotations.Broadcast;

/**
 * Emitter configuration.
 *
 * Using public fields for proxies.
 */
public class EmitterConfiguration {
    public String name;
    public OnOverflow.Strategy overflowBufferStrategy;
    public long overflowBufferSize;
    public boolean broadcast;
    public int numberOfSubscriberBeforeConnecting;

    public EmitterConfiguration() {
        // Used for proxies.
    }

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
