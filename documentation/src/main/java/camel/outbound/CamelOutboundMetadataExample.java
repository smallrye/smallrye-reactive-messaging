package camel.outbound;

import java.time.Duration;
import java.util.Random;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.camel.OutgoingExchangeMetadata;

@ApplicationScoped
public class CamelOutboundMetadataExample {

    private Random random = new Random();

    @Outgoing("prices")
    public Multi<Message<String>> generate() {
        // <code>
        return Multi.createFrom().ticks().every(Duration.ofSeconds(1))
                .map(x -> random.nextDouble())
                .map(p -> Double.toString(p))
                .map(s -> Message.of(s)
                        .addMetadata(new OutgoingExchangeMetadata()
                                .putProperty("my-property", "my-value")));
        // </code>
    }

}
