package sqs.outbound;

import java.time.Duration;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.aws.sqs.SqsOutboundMetadata;

@ApplicationScoped
public class SqsMessageStringProducer {

    @Outgoing("prices")
    public Multi<Message<String>> generate() {
        // It emits a UUID every second
        return Multi.createFrom().ticks().every(Duration.ofSeconds(1))
                .map(x -> Message.of(UUID.randomUUID().toString(),
                        Metadata.of(SqsOutboundMetadata.builder()
                                .groupId("group-1")
                                .build())));
    }
}
