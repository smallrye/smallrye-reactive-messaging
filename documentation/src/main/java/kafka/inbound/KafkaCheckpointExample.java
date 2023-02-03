package kafka.inbound;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.commit.CheckpointMetadata;

@ApplicationScoped
public class KafkaCheckpointExample {

    // <code>
    @Incoming("prices")
    public CompletionStage<Void> consume(KafkaRecord<String, Double> record) {
        // Get the `CheckpointMetadata` from the incoming message
        CheckpointMetadata<Double> checkpoint = CheckpointMetadata.fromMessage(record);

        // `CheckpointMetadata` allows transforming the processing state
        // Applies the given function, starting from the value `0.0` when no previous state exists
        checkpoint.transform(0.0, current -> current + record.getPayload(), /* persistOnAck */ true);

        // `persistOnAck` flag set to true, ack will persist the processing state
        // associated with the latest offset (per partition).
        return record.ack();
    }
    // </code>

}
