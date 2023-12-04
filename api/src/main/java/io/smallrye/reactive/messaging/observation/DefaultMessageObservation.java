package io.smallrye.reactive.messaging.observation;

import java.time.Duration;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * The default implementation based on system nano time.
 */
public class DefaultMessageObservation implements MessageObservation {

    // metadata
    private final String channelName;

    // time
    private final long creation;
    protected volatile long completion;

    // status
    protected volatile boolean done;
    protected volatile Throwable nackReason;

    public DefaultMessageObservation(String channelName) {
        this(channelName, System.nanoTime());
    }

    public DefaultMessageObservation(String channelName, long creationTime) {
        this.channelName = channelName;
        this.creation = creationTime;
    }

    @Override
    public String getChannel() {
        return channelName;
    }

    @Override
    public long getCreationTime() {
        return creation;
    }

    @Override
    public long getCompletionTime() {
        return completion;
    }

    @Override
    public boolean isDone() {
        return done || nackReason != null;
    }

    @Override
    public Throwable getReason() {
        return nackReason;
    }

    @Override
    public Duration getCompletionDuration() {
        if (isDone()) {
            return Duration.ofNanos(completion - creation);
        }
        return null;
    }

    @Override
    public void onMessageAck(Message<?> message) {
        completion = System.nanoTime();
        done = true;
    }

    @Override
    public void onMessageNack(Message<?> message, Throwable reason) {
        completion = System.nanoTime();
        nackReason = reason;
    }

}
