package kafka.inbound;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

@ApplicationScoped
public class KafkaDeadLetterExample {

    // <code>
    @Incoming("in")
    public CompletionStage<Void> consume(KafkaRecord<String, String> message) {
        return message.nack(new Exception("Failed!"), Metadata.of(
                OutgoingKafkaRecordMetadata.builder()
                        .withKey("failed-record")
                        .withHeaders(new RecordHeaders()
                                .add("my-header", "my-header-value".getBytes(StandardCharsets.UTF_8)))));
    }
    // </code>

}
