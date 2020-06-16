package inbound;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class KafkaRebalancedConsumer {

    @Incoming("rebalanced-example")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> consume(IncomingKafkaRecord<Integer, String> message) {
        // We don't need to ACK messages because in this example we set offset during consumer re-balance
        return CompletableFuture.completedFuture(null);
    }

}
