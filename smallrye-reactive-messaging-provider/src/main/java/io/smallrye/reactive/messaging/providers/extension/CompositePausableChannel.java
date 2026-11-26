package io.smallrye.reactive.messaging.providers.extension;

import java.time.Duration;
import java.util.List;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.PausableChannel;

/**
 * A {@link PausableChannel} grouping the pausable channels of a channel split into several parts, such as the
 * indexed channels ({@code channel$1}, {@code channel$2}, ...) created for an incoming channel configured with
 * {@code concurrency}.
 * <p>
 * It is registered under the base channel name, so the logical channel can be paused, resumed and drained as a whole.
 * The parts are still registered under their own names.
 */
public class CompositePausableChannel implements PausableChannel {

    private final List<PausableChannel> parts;

    public CompositePausableChannel(List<PausableChannel> parts) {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("A composite pausable channel needs at least one part");
        }
        this.parts = List.copyOf(parts);
    }

    /**
     * @return the pausable channels this composite groups
     */
    public List<PausableChannel> getParts() {
        return parts;
    }

    /**
     * @return {@code true} if every part is paused
     */
    @Override
    public boolean isPaused() {
        return parts.stream().allMatch(PausableChannel::isPaused);
    }

    @Override
    public void pause() {
        parts.forEach(PausableChannel::pause);
    }

    @Override
    public void resume() {
        parts.forEach(PausableChannel::resume);
    }

    @Override
    public void clearBuffer() {
        parts.forEach(PausableChannel::clearBuffer);
    }

    /**
     * Pauses and drains every part. Completes when all parts are drained.
     */
    @Override
    public Uni<Void> pauseAndDrain() {
        List<Uni<Void>> drains = parts.stream().map(PausableChannel::pauseAndDrain).toList();
        return Uni.join().all(drains).andCollectFailures().replaceWithVoid();
    }

    /**
     * @return the longest drain timeout of the parts, as they are drained concurrently
     */
    @Override
    public Duration getDrainTimeout() {
        return parts.stream().map(PausableChannel::getDrainTimeout).max(Duration::compareTo).orElseThrow();
    }

    /**
     * @return the number of in-flight messages across the parts, or {@code -1} if a part does not report it
     */
    public int getInFlightCount() {
        int total = 0;
        for (PausableChannel part : parts) {
            int count = inFlightCount(part);
            if (count < 0) {
                return -1;
            }
            total += count;
        }
        return total;
    }

    /**
     * @return the number of in-flight messages of the given channel, or {@code -1} if it does not report it, or does
     *         not track its in-flight messages
     */
    public static int inFlightCount(PausableChannel channel) {
        if (channel instanceof PausableChannelDecorator.PauserChannel pauser) {
            return pauser.isTracking() ? pauser.getInFlightCount() : -1;
        }
        if (channel instanceof CompositePausableChannel composite) {
            return composite.getInFlightCount();
        }
        return -1;
    }
}
