package acme;

import org.apache.kafka.common.network.Send;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {



    public static void main(String[] args) {
        SeContainer container = SeContainerInitializer.newInstance().initialize();
        final Sender sender = container.getBeanManager().createInstance().select(Sender.class).get();
        final Receiver receiver = container.getBeanManager().createInstance().select(Receiver.class).get();

        final AtomicInteger counter = new AtomicInteger();
        Executors.newSingleThreadScheduledExecutor()
            .scheduleAtFixedRate(() -> {
                    sender.send();
                },
                5, 10, TimeUnit.SECONDS);
    }
}
