package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.BasicProperties;

public class IncomingRabbitMQMetadataTest {

    @Test
    public void testHeaderWithNullValue() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("header1", "value1");
        properties.put("header2", null);

        IncomingRabbitMQMetadata incomingRabbitMQMetadata = new IncomingRabbitMQMetadata(new DummyBasicProperties(properties),
                null, null);

        assertThat(incomingRabbitMQMetadata.getHeaders().get("header1")).isEqualTo("value1");
        assertThat(incomingRabbitMQMetadata.getHeaders().containsKey("header2")).isTrue();
        assertThat(incomingRabbitMQMetadata.getHeaders().get("header2")).isNull();
    }

    class DummyBasicProperties implements BasicProperties {
        protected Map<String, Object> headers;

        DummyBasicProperties(Map<String, Object> headers) {
            this.headers = headers;
        }

        @Override
        public String getContentType() {
            return null;
        }

        @Override
        public String getContentEncoding() {
            return null;
        }

        @Override
        public Map<String, Object> getHeaders() {
            return headers;
        }

        @Override
        public Integer getDeliveryMode() {
            return null;
        }

        @Override
        public Integer getPriority() {
            return null;
        }

        @Override
        public String getCorrelationId() {
            return null;
        }

        @Override
        public String getReplyTo() {
            return null;
        }

        @Override
        public String getExpiration() {
            return null;
        }

        @Override
        public String getMessageId() {
            return null;
        }

        @Override
        public Date getTimestamp() {
            return null;
        }

        @Override
        public String getType() {
            return null;
        }

        @Override
        public String getUserId() {
            return null;
        }

        @Override
        public String getAppId() {
            return null;
        }
    }
}
