package sqs.json;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

public class SqsJsonMapping {

    // <code>
    @ApplicationScoped
    public static class Generator {

        @Outgoing("to-rabbitmq")
        public Multi<Price> prices() {
            AtomicInteger count = new AtomicInteger();
            return Multi.createFrom().ticks().every(Duration.ofMillis(1000))
                    .map(l -> new Price().setPrice(count.incrementAndGet()))
                    .onOverflow().drop();
        }

    }

    @ApplicationScoped
    public static class Consumer {

        List<Price> prices = new CopyOnWriteArrayList<>();

        @Incoming("from-rabbitmq")
        public void consume(Price price) {
            prices.add(price);
        }

        public List<Price> list() {
            return prices;
        }
    }

    public static class Price {
        public int price;

        public Price setPrice(int price) {
            this.price = price;
            return this;
        }
    }
    // </code>

}
