package acme;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class BeanUsingAnEmitter {

    @Inject
    @Channel("my-channel")
    Emitter<String> emitter;

    public void periodicallySendMessageToPulsar() {
        AtomicInteger counter = new AtomicInteger();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> {
                    emitter.send("Hello " + counter.getAndIncrement());
                },
                        1, 1, TimeUnit.SECONDS);
    }

}
