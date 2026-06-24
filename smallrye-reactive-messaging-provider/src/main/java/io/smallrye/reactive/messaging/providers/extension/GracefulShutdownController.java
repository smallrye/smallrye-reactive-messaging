package io.smallrye.reactive.messaging.providers.extension;

import java.time.Duration;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;

import io.smallrye.reactive.messaging.ChannelRegistry;

/**
 * Coordinates graceful shutdown of all channels.
 * <p>
 * Fires at {@code @Priority(40)} — before connectors ({@code @Priority(50)}) —
 * to pause and drain all pausable channels. By the time connectors run their
 * shutdown logic, all in-flight messages have been acknowledged.
 */
@ApplicationScoped
public class GracefulShutdownController {

    private static final Logger log = Logger.getLogger(GracefulShutdownController.class);

    @Inject
    ChannelRegistry registry;

    public void terminate(
            // Called before connector shutdown logic, which is @Priority(50)
            @Observes @Priority(40) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        for (var entry : registry.getPausableChannels().entrySet()) {
            try {
                Duration timeout = entry.getValue().getDrainTimeout();
                entry.getValue().pauseAndDrain().await().atMost(timeout);
            } catch (Exception e) {
                log.warnf(e, "Failed to drain channel '%s' during graceful shutdown", entry.getKey());
            }
        }
    }
}
