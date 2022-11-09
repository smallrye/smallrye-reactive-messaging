package io.smallrye.reactive.messaging.kafka.documentation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.kafka.LegacyMetadataTestUtils;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
public class KafkaPriceMessageConsumer {

    private final List<Double> list = new ArrayList<>();

    @SuppressWarnings({ "rawtypes" })
    @Incoming("prices")
    public CompletionStage<Void> consume(Message<Double> price) {
        // process your price.
        list.add(price.getPayload());
        Optional<IncomingKafkaRecordMetadata> metadata = price.getMetadata(IncomingKafkaRecordMetadata.class);
        metadata.orElseThrow(() -> new IllegalArgumentException("Metadata are missing"));
        LegacyMetadataTestUtils.tempCompareLegacyAndApiMetadata(metadata.get(), price);
        // Acknowledge the incoming message (commit the offset)
        return price.ack();
    }

    public List<Double> list() {
        return list;
    }

}
