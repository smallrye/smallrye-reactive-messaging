package shutdown;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

// <emitter>
@ApplicationScoped
public class EmitterShutdownExample {

    @Inject
    @Channel("out")
    Emitter<String> emitter;

    public void send(String value) {
        emitter.send(value);
    }

    // During graceful shutdown, the emitter is completed,
    // flushing any buffered messages to the outgoing connector.
    // After shutdown, calling send() will throw an exception.
}
// </emitter>
