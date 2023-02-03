package acme;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class BeanUsingAnEmitter {

    @Inject
    @Channel("my-channel")
    Emitter<String> emitter;

    public void periodicallySendMessage() {
        AtomicInteger counter = new AtomicInteger();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> {
                    String message = "Hello " + counter.getAndIncrement();
                    System.out.println("Emitting: " + message);
                    emitter.send(message);
                },
                        1, 1, TimeUnit.SECONDS);
    }

}
