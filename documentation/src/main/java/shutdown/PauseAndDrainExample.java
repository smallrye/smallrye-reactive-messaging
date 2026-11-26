package shutdown;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.reactive.messaging.PausableChannel;

// <pause-drain>
@ApplicationScoped
public class PauseAndDrainExample {

    @Inject
    @Channel("data")
    PausableChannel channel;

    public void pauseAndWait() {
        // Pauses the channel and waits for all in-flight messages
        // to be acknowledged or negatively acknowledged.
        channel.pauseAndDrain()
                .await().atMost(channel.getDrainTimeout());
    }
}
// </pause-drain>
