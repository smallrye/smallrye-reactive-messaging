package io.smallrye.reactive.messaging.kafka;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.Assert;

/**
 * Delete once we have got rid of the legacy {@link io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata}
 * and {@link IncomingKafkaRecordMetadata} implementations
 */
@SuppressWarnings({ "deprecation", "OptionalGetWithoutIsPresent" })
public class LegacyMetadataTestUtils {

    public static void tempCompareLegacyAndApiMetadata(
            io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata<?, ?> api, Message<?> msg) {
        IncomingKafkaRecordMetadata<?, ?> legacy = msg
                .getMetadata(IncomingKafkaRecordMetadata.class)
                .get();
        tempCompareMetadata(api, legacy);
        Assert.assertEquals(api.getOffset(), legacy.getOffset());
        Assert.assertEquals(api.getTimestampType(), legacy.getTimestampType());

    }

    public static void tempCompareLegacyAndApiMetadata(
            io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata<?> api, Message<?> msg) {
        OutgoingKafkaRecordMetadata<?> legacy = msg
                .getMetadata(OutgoingKafkaRecordMetadata.class)
                .get();
        tempCompareMetadata(api, legacy);
    }

    private static void tempCompareMetadata(io.smallrye.reactive.messaging.kafka.api.KafkaMessageMetadata<?> api,
            KafkaMessageMetadata<?> legacy) {
        Assert.assertEquals(api.getKey(), legacy.getKey());
        Assert.assertEquals(api.getTopic(), legacy.getTopic());
        Assert.assertEquals(api.getPartition(), legacy.getPartition());
        Assert.assertEquals(api.getTimestamp(), legacy.getTimestamp());
        Assert.assertEquals(api.getHeaders(), legacy.getHeaders());
    }
}
