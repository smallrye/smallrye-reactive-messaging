package io.smallrye.reactive.messaging.jms.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class MessagePropertiesExtractAdapterTest {
    @Test
    public void verifyNullHeaderHandled() {
        Map<String, Object> messageProperties = new HashMap<>();
        messageProperties.put("test_null_header", null);

        JmsTrace jmsTrace = new JmsTrace.Builder().withProperties(messageProperties).build();

        String headerValue = JmsTraceTextMapGetter.INSTANCE.get(jmsTrace, "test_null_header");

        assertThat(headerValue).isNull();
    }
}
