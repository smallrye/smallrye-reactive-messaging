package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;

class SqsMessageTypeConverterTest {

    private final SqsMessageTypeConverter converter = new SqsMessageTypeConverter();

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();

    private final software.amazon.awssdk.services.sqs.model.Message sqsMessage = software.amazon.awssdk.services.sqs.model.Message
            .builder().messageId("id-1").body("hello").build();

    /**
     * A message as rebuilt by an interception: connector metadata and acknowledgement kept, connector type lost.
     */
    private Message<String> intercepted() {
        return Message.of("hello", Metadata.of(new SqsIncomingMetadata(sqsMessage)),
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

        assertThat(converter.canConvert(message, SqsMessage.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, Message.class)).isFalse();
        // Not a message of the connector.
        assertThat(converter.canConvert(Message.of("hello"), SqsMessage.class)).isFalse();
        // Already restored: nothing to restore.
        Message<?> restored = converter.convert(message, SqsMessage.class);
        assertThat(converter.canConvert(restored, SqsMessage.class)).isFalse();
    }

    @Test
    void restoresTheConnectorMessageAndDelegatesAcknowledgement() {
        Message<String> message = intercepted();

        Message<?> converted = converter.convert(message, SqsMessage.class);

        assertThat(converted).isInstanceOf(SqsMessage.class);
        SqsMessage<?> restored = (SqsMessage<?>) converted;
        assertThat(restored.getPayload()).isEqualTo("hello");
        assertThat(restored.getMetadata()).isSameAs(message.getMetadata());
        assertThat(restored.getMessage()).isSameAs(sqsMessage);

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
        assertThat(converter.canConvert(tracked, SqsMessage.class)).isTrue();

        SqsMessage<?> restored = (SqsMessage<?>) converter.convert(tracked, SqsMessage.class);

        // Acknowledging the restored message acknowledges the tracked message.
        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        assertThat(channel.getInFlightCount()).isZero();
    }
}
