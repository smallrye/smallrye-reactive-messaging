package io.smallrye.reactive.messaging.providers.extension;

import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import io.smallrye.reactive.messaging.annotations.Broadcast;

/**
 * Emitter configuration.
 *
 * Using public fields for proxies.
 */
public class EmitterConfiguration {
    public String name;
    public boolean isMutinyEmitter;
    public OnOverflow.Strategy overflowBufferStrategy;
    public long overflowBufferSize;
    public boolean broadcast;
    public int numberOfSubscriberBeforeConnecting;

    public EmitterConfiguration() {
        // Used for proxies.
    }

    public EmitterConfiguration(String name, boolean isMutinyEmitter, OnOverflow onOverflow, Broadcast broadcast) {
        this.name = name;
        this.isMutinyEmitter = isMutinyEmitter;

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
}
