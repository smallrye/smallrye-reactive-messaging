package acme;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class Sender {

    @Outgoing("to-rabbitmq-string")
    public Multi<String> pricesString() {
        AtomicInteger count = new AtomicInteger();
        return Multi.createFrom().ticks().every(Duration.ofMillis(1000))
                .map(l -> String.valueOf(count.getAndIncrement()))
                .onOverflow().drop();
    }

    @Outgoing("to-rabbitmq-jsonobject")
    public Multi<Price> prices() {
        AtomicInteger count = new AtomicInteger();
        return Multi.createFrom().ticks().every(Duration.ofMillis(1000))
                .map(l -> {
                    Price p = new Price();
                    p.price = count.getAndIncrement();
                    return p;
                })
                .onOverflow().drop();
    }

}
