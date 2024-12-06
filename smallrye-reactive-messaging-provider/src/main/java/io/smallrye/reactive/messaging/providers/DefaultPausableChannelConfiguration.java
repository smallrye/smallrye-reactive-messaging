package io.smallrye.reactive.messaging.providers;

import java.util.Objects;

import io.smallrye.reactive.messaging.PausableChannelConfiguration;

/**
 * Default implementation of {@link PausableChannelConfiguration}.
 */
public class DefaultPausableChannelConfiguration implements PausableChannelConfiguration {

    private final String name;
    private final boolean initiallyPaused;
    private final boolean lateSubscription;
    private final boolean bufferAlreadyRequested;

    public DefaultPausableChannelConfiguration(String name,
            boolean initiallyPaused,
            boolean lateSubscription,
            boolean bufferAlreadyRequested) {
        this.name = name;
        this.initiallyPaused = initiallyPaused;
        this.lateSubscription = lateSubscription;
        this.bufferAlreadyRequested = bufferAlreadyRequested;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean initiallyPaused() {
        return initiallyPaused;
    }

    @Override
    public boolean lateSubscription() {
        return lateSubscription;
    }

    @Override
    public boolean bufferAlreadyRequested() {
        return bufferAlreadyRequested;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DefaultPausableChannelConfiguration))
            return false;
        DefaultPausableChannelConfiguration that = (DefaultPausableChannelConfiguration) o;
        return initiallyPaused == that.initiallyPaused
                && lateSubscription == that.lateSubscription
                && bufferAlreadyRequested == that.bufferAlreadyRequested
                && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, initiallyPaused, lateSubscription, bufferAlreadyRequested);
    }

    @Override
    public String toString() {
        return "DefaultPausableChannelConfiguration{" +
                "name='" + name + '\'' +
                ", initialPaused=" + initiallyPaused +
                ", lateSubscription=" + lateSubscription +
                ", bufferAlreadyRequested=" + bufferAlreadyRequested +
                '}';
    }
}
