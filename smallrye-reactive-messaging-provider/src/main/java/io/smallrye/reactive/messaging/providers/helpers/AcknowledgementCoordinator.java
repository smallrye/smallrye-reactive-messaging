package io.smallrye.reactive.messaging.providers.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * A utility class than orchestrate the (negative-)acknowledgment of a specified message based on the ack/nack of a set of
 * messages.
 * <p>
 * A coordinator is created with a specific input message.
 * For each message that needs to be tracked, the {@code track} method is called, which returned a modified message.
 * When all the added messages are acked, the coordinator acks the input message.
 * When one of the added message is nacked, the coordinator nacks the input message with the same reason.
 */
public class AcknowledgementCoordinator {

    private final Message<?> input;

    private volatile boolean done;
    private final List<Tracker> tracked = new ArrayList<>();

    private final ReentrantLock lock = new ReentrantLock();

    public AcknowledgementCoordinator(Message<?> input) {
        this.input = input;
    }

    public Message<?> track(Message<?> msg) {
        lock.lock();
        try {
            Tracker tracker = new Tracker();
            tracked.add(tracker);
            return msg
                    .withAck(() -> {
                        onAck(tracker);
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(reason -> {
                        onNack(reason, tracker);
                        return CompletableFuture.completedFuture(null);
                    });
        } finally {
            lock.unlock();
        }
    }

    private void onAck(Tracker id) {
        lock.lock();
        try {
            if (done) {
                return;
            }
            if (tracked.remove(id)) {
                if (tracked.isEmpty() && !done) {
                    // Done!
                    done = true;
                    input.ack();
                }
                // Otherwise not done yet.
            }
        } finally {
            lock.unlock();
        }
        // Already acked or nack.
    }

    private void onNack(Throwable reason, Tracker id) {
        lock.lock();
        try {
            if (done) {
                return;
            }
            if (tracked.remove(id)) {
                done = true;
                tracked.clear();
                input.nack(reason);
            }
        } finally {
            lock.unlock();
        }
    }

    static class Tracker {

    }
}
