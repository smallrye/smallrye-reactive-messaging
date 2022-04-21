package io.smallrye.reactive.messaging.providers.wiring;

import jakarta.enterprise.inject.UnsatisfiedResolutionException;

public class WiringException extends UnsatisfiedResolutionException {
    public WiringException() {
    }

    public WiringException(final String message) {
        super(message);
    }
}
