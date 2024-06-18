package io.smallrye.reactive.messaging.providers;

import java.util.Objects;

import io.smallrye.reactive.messaging.PausableChannelConfiguration;

/**
 * Default implementation of {@link PausableChannelConfiguration}.
 */
public class DefaultPausableChannelConfiguration implements PausableChannelConfiguration {

    private final String name;
    private final boolean initiallyPaused;

    public DefaultPausableChannelConfiguration(String name, boolean initiallyPaused) {
        this.name = name;
        this.initiallyPaused = initiallyPaused;
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
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DefaultPausableChannelConfiguration))
            return false;
        DefaultPausableChannelConfiguration that = (DefaultPausableChannelConfiguration) o;
        return initiallyPaused == that.initiallyPaused && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, initiallyPaused);
    }

    @Override
    public String toString() {
        return "DefaultPausableChannelConfiguration{" +
                "name='" + name + '\'' +
                ", initialPaused=" + initiallyPaused +
                '}';
    }
}
