package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;
import io.vertx.amqp.impl.AmqpMessageImpl;

class AmqpMessageTypeConverterTest {

    private final AmqpMessageTypeConverter converter = new AmqpMessageTypeConverter();

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();

    private final IncomingAmqpMetadata amqpMetadata;

    AmqpMessageTypeConverterTest() {
        org.apache.qpid.proton.message.Message proton = org.apache.qpid.proton.message.Message.Factory.create();
        proton.setAddress("the-address");
        proton.setSubject("the-subject");
        amqpMetadata = new IncomingAmqpMetadata(new AmqpMessageImpl(proton));
    }

    /**
     * A message as rebuilt by an interception: connector metadata and acknowledgement kept, connector type lost.
     */
    private Message<String> intercepted() {
        return Message.of("hello", Metadata.of(amqpMetadata),
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

        assertThat(converter.canConvert(message, AmqpMessage.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, Message.class)).isFalse();
        // Not a message of the connector.
        assertThat(converter.canConvert(Message.of("hello"), AmqpMessage.class)).isFalse();
        // Already restored: nothing to restore.
        Message<?> restored = converter.convert(message, AmqpMessage.class);
        assertThat(converter.canConvert(restored, AmqpMessage.class)).isFalse();
    }

    @Test
    void restoresTheConnectorMessageAndDelegatesAcknowledgement() {
        Message<String> message = intercepted();

        Message<?> converted = converter.convert(message, AmqpMessage.class);

        assertThat(converted).isInstanceOf(AmqpMessage.class);
        AmqpMessage<?> restored = (AmqpMessage<?>) converted;
        assertThat(restored.getPayload()).isEqualTo("hello");
        assertThat(restored.getMetadata()).isSameAs(message.getMetadata());
        assertThat(restored.getAddress()).isEqualTo("the-address");
        assertThat(restored.getSubject()).isEqualTo("the-subject");

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
        assertThat(converter.canConvert(tracked, AmqpMessage.class)).isTrue();

        AmqpMessage<?> restored = (AmqpMessage<?>) converter.convert(tracked, AmqpMessage.class);

        // Acknowledging the restored message acknowledges the tracked message.
        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        assertThat(channel.getInFlightCount()).isZero();
    }
}
