package pausable;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;

@ApplicationScoped
public class PausableController {

    @Inject
    ChannelRegistry registry;

    @PostConstruct
    public void resume() {
        // Wait for the application to be ready
        // Retrieve the pausable channel
        PausableChannel pausable = registry.getPausable("my-channel");
        // Pause the processing of the messages
        pausable.resume();
    }

    public void pause() {
        // Retrieve the pausable channel
        PausableChannel pausable = registry.getPausable("my-channel");
        // Pause the processing of the messages
        pausable.pause();
    }

    @Incoming("my-channel")
    void process(String message) {
        // Process the message
    }

}
