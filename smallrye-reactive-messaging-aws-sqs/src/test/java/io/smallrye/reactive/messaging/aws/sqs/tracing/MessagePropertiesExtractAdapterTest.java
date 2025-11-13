package io.smallrye.reactive.messaging.aws.sqs.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

class MessagePropertiesExtractAdapterTest {
    @Test
    public void verifyNullHeaderHandled() {
        Map<String, MessageAttributeValue> messageProperties = new HashMap<>();
        messageProperties.put("test_null_header", null);

        SqsTrace sqsTrace = new SqsTrace("", messageProperties);

        String headerValue = SqsTraceTextMapGetter.INSTANCE.get(sqsTrace, "test_null_header");
        SqsTraceTextMapSetter.INSTANCE.set(sqsTrace, "test_other_header", "value");

        assertThat(headerValue).isNull();
        assertThat(messageProperties.get("test_other_header").stringValue()).isEqualTo("value");
    }
}
