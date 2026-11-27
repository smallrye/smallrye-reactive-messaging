package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;
import io.vertx.core.buffer.Buffer;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

class MqttMessageTypeConverterTest {

    private final MqttMessageTypeConverter converter = new MqttMessageTypeConverter();

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();

    private static final byte[] PAYLOAD = "hello".getBytes(StandardCharsets.UTF_8);

    private final ReceivingMqttMessageMetadata mqttMetadata = new ReceivingMqttMessageMetadata(
            MqttPublishMessage.newInstance(io.vertx.mqtt.messages.MqttPublishMessage.create(7, MqttQoS.AT_LEAST_ONCE,
                    false, false, "the-topic", Buffer.buffer(PAYLOAD))));

    /**
     * A message as rebuilt by an interception: connector metadata and acknowledgement kept, connector type lost.
     */
    private Message<byte[]> intercepted() {
        return Message.of(PAYLOAD, Metadata.of(mqttMetadata),
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
        Message<byte[]> message = intercepted();

        assertThat(converter.canConvert(message, ReceivingMqttMessage.class)).isTrue();
        assertThat(converter.canConvert(message, MqttMessage.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, Message.class)).isFalse();
        // Not a message of the connector.
        assertThat(converter.canConvert(Message.of(PAYLOAD), ReceivingMqttMessage.class)).isFalse();
        // Already restored: nothing to restore.
        Message<?> restored = converter.convert(message, ReceivingMqttMessage.class);
        assertThat(converter.canConvert(restored, ReceivingMqttMessage.class)).isFalse();
    }

    @Test
    void restoresTheConnectorMessageAndDelegatesAcknowledgement() {
        Message<byte[]> message = intercepted();

        Message<?> converted = converter.convert(message, ReceivingMqttMessage.class);

        assertThat(converted).isInstanceOf(ReceivingMqttMessage.class);
        ReceivingMqttMessage restored = (ReceivingMqttMessage) converted;
        assertThat(restored.getPayload()).isEqualTo(PAYLOAD);
        assertThat(restored.getMetadata()).isSameAs(message.getMetadata());
        assertThat(restored.getTopic()).isEqualTo("the-topic");
        assertThat(restored.getMessageId()).isEqualTo(7);

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
        assertThat(converter.canConvert(tracked, ReceivingMqttMessage.class)).isTrue();

        ReceivingMqttMessage restored = (ReceivingMqttMessage) converter.convert(tracked, ReceivingMqttMessage.class);

        // Acknowledging the restored message acknowledges the tracked message.
        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        assertThat(channel.getInFlightCount()).isZero();
    }
}
