package inbound;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.Record;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class KafkaDeadLetterExample {

    // tag::code[]
    @Incoming("in")
    public CompletionStage<Void> consume(KafkaRecord<String, String> message) {
        return message.nack(new Exception("Failed!"), Metadata.of(
            OutgoingKafkaRecordMetadata.builder()
                .withKey("failed-record")
                .withHeaders(new RecordHeaders()
                    .add("my-header", "my-header-value".getBytes(StandardCharsets.UTF_8))
                )
        ));
    }
    // end::code[]

}
