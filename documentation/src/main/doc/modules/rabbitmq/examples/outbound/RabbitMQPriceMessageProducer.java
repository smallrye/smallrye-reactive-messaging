package outbound;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Random;

@ApplicationScoped
public class RabbitMQPriceMessageProducer {

    private Random random = new Random();

    @Outgoing("prices")
    public Multi<Message<Double>> generate() {
        // Build an infinite stream of random prices
        // It emits a price every second
        return Multi.createFrom().ticks().every(Duration.ofSeconds(1))
                .map(x -> Message.of(random.nextDouble(),
                        Metadata.of(new OutgoingRabbitMQMetadata.Builder()
                                .withRoutingKey("normal")
                                .withTimestamp(ZonedDateTime.now())
                                .build())
                ));
    }

}
