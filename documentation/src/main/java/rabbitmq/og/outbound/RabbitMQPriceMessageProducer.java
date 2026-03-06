package rabbitmq.og.outbound;

import java.time.Duration;
import java.util.Date;
import java.util.Random;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.rabbitmq.og.OutgoingRabbitMQMetadata;

@ApplicationScoped
public class RabbitMQPriceMessageProducer {

    private Random random = new Random();

    @Outgoing("prices")
    public Multi<Message<Double>> generate() {
        // Build an infinite stream of random prices
        // It emits a price every second
        return Multi.createFrom().ticks().every(Duration.ofSeconds(1))
                .map(x -> Message.of(random.nextDouble(),
                        Metadata.of(OutgoingRabbitMQMetadata.builder()
                                .withRoutingKey("normal")
                                .withTimestamp(new Date())
                                .build())));
    }

}
