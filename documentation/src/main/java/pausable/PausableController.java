package pausable;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;

@ApplicationScoped
public class PausableController {

    @Inject
    @Channel("my-channel")
    PausableChannel pausable;

    @Inject
    ChannelRegistry registry;

    public PausableChannel getPausable() {
        // Retrieve the pausable channel from channel registry
        return registry.getPausable("my-channel");
    }

    @PostConstruct
    public void resume() {
        // Wait for the application to be ready
        // Pause the processing of the messages
        pausable.resume();
    }

    public void pause() {
        // Pause the processing of the messages
        pausable.pause();
    }

    @Incoming("my-channel")
    void process(String message) {
        // Process the message
    }

}
