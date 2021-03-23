package io.smallrye.reactive.messaging.amqp;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

class OutgoingAmqpMetadataTest {

    @Test
    void testMetadata() {
        OutgoingAmqpMetadata metadata = new OutgoingAmqpMetadata(
                "address",
                false,
                (short) 3,
                1000,
                null,
                null,
                "id",
                "user-id",
                "subject",
                null,
                "correlation-id",
                "text/plain",
                "utf-8",
                1000,
                System.currentTimeMillis(),
                "group-id",
                1001,
                "reply-group",
                new JsonObject(),
                null);

        assertEquals(1000, metadata.getExpiryTime());
        assertTrue(metadata.getCreationTime() > 0);
        assertTrue(metadata.getGroupSequence() > 0);
    }

    @Test
    void testCopy() {
        OutgoingAmqpMetadata metadata = new OutgoingAmqpMetadata(
                "address",
                false,
                (short) 3,
                1000,
                null,
                null,
                "id",
                "user-id",
                "subject",
                null,
                "correlation-id",
                "text/plain",
                "utf-8",
                1000,
                System.currentTimeMillis(),
                "group-id",
                1001,
                "reply-group",
                new JsonObject(),
                null);

        OutgoingAmqpMetadata.OutgoingAmqpMetadataBuilder builder = OutgoingAmqpMetadata.from(metadata);
        assertNotNull(builder);

        builder.withDeliveryAnnotations(new DeliveryAnnotations(Collections.singletonMap(Symbol.getSymbol("key"), "value")));
        builder.withMessageAnnotations(new MessageAnnotations(Collections.singletonMap(Symbol.getSymbol("k"), "v")));
        builder.withExpiryTime(2000);
        builder.withCreationTime(System.currentTimeMillis());
        builder.withGroupSequence(2002);
        builder.withApplicationProperty("ap", "ap");

        OutgoingAmqpMetadata result = builder.build();
        assertEquals(2000, result.getExpiryTime());
        assertTrue(result.getCreationTime() > 0);
        assertTrue(result.getGroupSequence() > 0);
        assertEquals(result.getProperties().size(), 1);
        assertEquals(result.getDeliveryAnnotations().getValue().size(), 1);
        assertEquals(result.getMessageAnnotations().getValue().size(), 1);
    }

}
