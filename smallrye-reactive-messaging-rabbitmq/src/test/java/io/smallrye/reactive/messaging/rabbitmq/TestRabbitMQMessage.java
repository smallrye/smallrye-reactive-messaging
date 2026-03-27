package io.smallrye.reactive.messaging.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Envelope;

import io.vertx.core.buffer.Buffer;
import io.vertx.rabbitmq.RabbitMQMessage;

/**
 * A simple concrete implementation of {@link RabbitMQMessage} for unit tests,
 * avoiding the need for Mockito mocks.
 */
public class TestRabbitMQMessage implements RabbitMQMessage {

    private final Buffer body;
    private final BasicProperties properties;
    private final Envelope envelope;
    private final String consumerTag;
    private final Integer messageCount;

    private TestRabbitMQMessage(Buffer body, BasicProperties properties, Envelope envelope,
            String consumerTag, Integer messageCount) {
        this.body = body;
        this.properties = properties;
        this.envelope = envelope;
        this.consumerTag = consumerTag;
        this.messageCount = messageCount;
    }

    @Override
    public Buffer body() {
        return body;
    }

    @Override
    public String consumerTag() {
        return consumerTag;
    }

    @Override
    public Envelope envelope() {
        return envelope;
    }

    @Override
    public BasicProperties properties() {
        return properties;
    }

    @Override
    public Integer messageCount() {
        return messageCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Buffer body = Buffer.buffer();
        private BasicProperties properties = new AMQP.BasicProperties();
        private Envelope envelope = new Envelope(1L, false, "test-exchange", "test-key");
        private String consumerTag = "test-consumer";
        private Integer messageCount = 0;

        public Builder body(Buffer body) {
            this.body = body;
            return this;
        }

        public Builder body(String body) {
            this.body = Buffer.buffer(body);
            return this;
        }

        public Builder body(byte[] body) {
            this.body = Buffer.buffer(body);
            return this;
        }

        public Builder properties(BasicProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder envelope(Envelope envelope) {
            this.envelope = envelope;
            return this;
        }

        public Builder envelope(long deliveryTag, boolean redeliver, String exchange, String routingKey) {
            this.envelope = new Envelope(deliveryTag, redeliver, exchange, routingKey);
            return this;
        }

        public Builder consumerTag(String consumerTag) {
            this.consumerTag = consumerTag;
            return this;
        }

        public Builder messageCount(Integer messageCount) {
            this.messageCount = messageCount;
            return this;
        }

        public TestRabbitMQMessage build() {
            return new TestRabbitMQMessage(body, properties, envelope, consumerTag, messageCount);
        }
    }
}
