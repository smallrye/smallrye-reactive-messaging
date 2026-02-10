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

    // Option 1: Inject ChannelRegistry and retrieve the pausable channel
    @Inject
    ChannelRegistry registry;

    // Option 2: Directly inject the pausable channel
    @Inject
    @Channel("my-channel")
    PausableChannel myChannel;

    @PostConstruct
    public void resume() {
        // Wait for the application to be ready
        // Option 1: Retrieve the pausable channel from the registry
        PausableChannel pausable = registry.getPausable("my-channel");
        // Resume the processing of the messages
        pausable.resume();
    }

    public void pause() {
        // Option 2: Use the injected pausable channel

        // Pause the processing of the messages
        myChannel.pause();
    }

    @Incoming("my-channel")
    void process(String message) {
        // Process the message
    }

}
