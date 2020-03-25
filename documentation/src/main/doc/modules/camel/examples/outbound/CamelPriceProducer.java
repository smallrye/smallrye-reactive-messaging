package outbound;

import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Random;

@ApplicationScoped
public class CamelPriceProducer {

    private Random random = new Random();

    @Outgoing("prices")
    public Multi<String> generate() {
        // Build an infinite stream of random prices
        return Multi.createFrom().ticks().every(Duration.ofSeconds(1))
            .on().overflow().drop()
            .map(x -> random.nextDouble())
            .map(p -> Double.toString(p));
    }

}
