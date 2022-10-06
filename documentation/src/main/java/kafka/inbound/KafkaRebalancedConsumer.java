package kafka.inbound;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

@ApplicationScoped
public class KafkaRebalancedConsumer {

    @Incoming("rebalanced-example")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> consume(IncomingKafkaRecord<Integer, String> message) {
        // We don't need to ACK messages because in this example we set offset during consumer re-balance
        return CompletableFuture.completedFuture(null);
    }

}
