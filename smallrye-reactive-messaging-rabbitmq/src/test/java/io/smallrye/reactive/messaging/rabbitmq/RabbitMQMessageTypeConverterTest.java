package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;

class RabbitMQMessageTypeConverterTest {

    private final RabbitMQMessageTypeConverter converter = new RabbitMQMessageTypeConverter();

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();

    private final IncomingRabbitMQMetadata rabbitMQMetadata = new IncomingRabbitMQMetadata(
            new Envelope(1L, false, "the-exchange", "the-key"),
            new AMQP.BasicProperties.Builder().correlationId("corr-1").build(),
            "hello".getBytes(StandardCharsets.UTF_8), null);

    /**
     * A message as rebuilt by an interception: connector metadata and acknowledgement kept, connector type lost.
     */
    private Message<String> intercepted() {
        return Message.of("hello", Metadata.of(rabbitMQMetadata),
                () -> {
                    acks.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                failure -> {
                    nacks.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
    }

    @Test
    void canConvertOnlyTheConnectorTypeOfInterceptedMessages() {
        Message<String> message = intercepted();

        assertThat(converter.canConvert(message, IncomingRabbitMQMessage.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, Message.class)).isFalse();
        // Not a message of the connector.
        assertThat(converter.canConvert(Message.of("hello"), IncomingRabbitMQMessage.class)).isFalse();
        // Already restored: nothing to restore.
        Message<?> restored = converter.convert(message, IncomingRabbitMQMessage.class);
        assertThat(converter.canConvert(restored, IncomingRabbitMQMessage.class)).isFalse();
    }

    @Test
    void restoresTheConnectorMessageAndDelegatesAcknowledgement() {
        Message<String> message = intercepted();

        Message<?> converted = converter.convert(message, IncomingRabbitMQMessage.class);

        assertThat(converted).isInstanceOf(IncomingRabbitMQMessage.class);
        IncomingRabbitMQMessage<?> restored = (IncomingRabbitMQMessage<?>) converted;
        assertThat(restored.getPayload()).isEqualTo("hello");
        assertThat(restored.getMetadata()).isSameAs(message.getMetadata());
        assertThat(restored.getRabbitMQMetadata()).isSameAs(rabbitMQMetadata);
        assertThat(restored.getCorrelationId()).hasValue("corr-1");

        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        restored.getAckWithMetadata().apply(Metadata.empty()).toCompletableFuture().join();
        assertThat(acks).hasValue(2);
        restored.nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(nacks).hasValue(1);
        restored.getNackWithMetadata().apply(new Exception("boom"), Metadata.empty()).toCompletableFuture().join();
        assertThat(nacks).hasValue(2);
    }

    @Test
    void restoresATrackedMessageAndKeepsTheTracking() {
        PauserChannel channel = new PauserChannel(new DemandPauser(), 1000, null, true);
        Message<?> tracked = channel.trackMessage(intercepted());
        assertThat(channel.getInFlightCount()).isEqualTo(1);
        assertThat(converter.canConvert(tracked, IncomingRabbitMQMessage.class)).isTrue();

        IncomingRabbitMQMessage<?> restored = (IncomingRabbitMQMessage<?>) converter.convert(tracked,
                IncomingRabbitMQMessage.class);

        // Acknowledging the restored message acknowledges the tracked message.
        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        assertThat(channel.getInFlightCount()).isZero();
    }
}
