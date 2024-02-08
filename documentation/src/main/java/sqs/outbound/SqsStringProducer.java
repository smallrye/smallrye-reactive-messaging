package sqs.outbound;

import java.time.Duration;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class SqsStringProducer {

    @Outgoing("data")
    public Multi<String> generate() {
        // It emits a UUID every second
        return Multi.createFrom().ticks().every(Duration.ofSeconds(1))
                .map(x -> UUID.randomUUID().toString());
    }

}
