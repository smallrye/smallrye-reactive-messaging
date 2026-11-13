package shutdown;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.annotations.Blocking;

// <consumer>
@ApplicationScoped
public class GracefulShutdownExample {

    @Incoming("data")
    @Blocking
    public void process(String payload) {
        // During graceful shutdown, the channel is paused
        // and in-flight messages are allowed to complete.
        // No new messages will be delivered.
    }
}
// </consumer>
