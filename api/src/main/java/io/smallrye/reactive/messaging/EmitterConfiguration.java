package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;

public interface EmitterConfiguration {

    String name();

    EmitterFactoryFor emitterType();

    OnOverflow.Strategy overflowBufferStrategy();

    long overflowBufferSize();

    boolean broadcast();

    int numberOfSubscriberBeforeConnecting();
}
