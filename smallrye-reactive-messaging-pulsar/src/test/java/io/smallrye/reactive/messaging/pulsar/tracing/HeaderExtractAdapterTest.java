package io.smallrye.reactive.messaging.pulsar.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.junit.jupiter.api.Test;

class HeaderExtractAdapterTest {
    @Test
    public void verifyNullHeaderHandled() {
        Map<String, String> properties = new HashMap<>();
        properties.put("test_null_header", null);

        MessageImpl<String> message = new MessageImpl<>("topic", MessageId.latest.toString(), properties, "payload".getBytes(),
                Schema.STRING,
                new MessageMetadata());
        PulsarTrace kafkaTrace = new PulsarTrace.Builder().withConsumerName("consumer-name").withMessage(message).build();

        String headerValue = PulsarTraceTextMapGetter.INSTANCE.get(kafkaTrace, "test_null_header");

        assertThat(headerValue).isNull();
    }
}
