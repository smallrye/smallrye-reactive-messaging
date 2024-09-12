package io.smallrye.reactive.messaging.jms.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import jakarta.jms.JMSException;
import jakarta.jms.Message;

import org.junit.jupiter.api.Test;

class MessagePropertiesExtractAdapterTest {
    @Test
    public void verifyNullHeaderHandled() throws JMSException {
        Map<String, Object> messageProperties = new HashMap<>();
        messageProperties.put("test_null_header", null);

        // Create a mock JMS message
        Message msg = mock(Message.class);
        doAnswer(i -> messageProperties.get(i.getArgument(0, String.class)))
                .when(msg).getStringProperty(anyString());
        doAnswer(i -> messageProperties.put(i.getArgument(0, String.class), i.getArgument(1, String.class)))
                .when(msg).setStringProperty(anyString(), anyString());

        JmsTrace jmsTrace = new JmsTrace.Builder().withMessage(msg).build();

        String headerValue = JmsTraceTextMapGetter.INSTANCE.get(jmsTrace, "test_null_header");
        JmsTraceTextMapSetter.INSTANCE.set(jmsTrace, "test_other_header", "value");

        assertThat(headerValue).isNull();
        assertThat(messageProperties.get("test_other_header")).isEqualTo("value");
    }
}
