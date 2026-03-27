package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;

public class IncomingRabbitMQMetadataTest {

    @Test
    public void testHeaderWithNullValue() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("header1", "value1");
        headers.put("header2", null);

        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .headers(headers)
                .build();

        IncomingRabbitMQMetadata incomingRabbitMQMetadata = new IncomingRabbitMQMetadata(null, properties);

        assertThat(incomingRabbitMQMetadata.getHeaders().get("header1")).isEqualTo("value1");
        assertThat(incomingRabbitMQMetadata.getHeaders().containsKey("header2")).isTrue();
        assertThat(incomingRabbitMQMetadata.getHeaders().get("header2")).isNull();
    }
}
