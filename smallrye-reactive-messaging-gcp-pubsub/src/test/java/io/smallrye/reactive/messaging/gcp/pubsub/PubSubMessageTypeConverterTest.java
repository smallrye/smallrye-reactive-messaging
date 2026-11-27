package io.smallrye.reactive.messaging.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;

class PubSubMessageTypeConverterTest {

    private final PubSubMessageTypeConverter converter = new PubSubMessageTypeConverter();

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();

    private final PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("hello"))
            .putAttributes("key", "value")
            .build();

    /**
     * A message as rebuilt by an interception: connector metadata and acknowledgement kept, connector type lost.
     */
    private Message<String> intercepted() {
        return Message.of("hello", Metadata.of(new IncomingPubSubMetadata(pubsubMessage)),
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

        assertThat(converter.canConvert(message, PubSubMessage.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, Message.class)).isFalse();
        // Not a message of the connector.
        assertThat(converter.canConvert(Message.of("hello"), PubSubMessage.class)).isFalse();
        // Already restored: nothing to restore.
        Message<?> restored = converter.convert(message, PubSubMessage.class);
        assertThat(converter.canConvert(restored, PubSubMessage.class)).isFalse();
    }

    @Test
    void restoresTheConnectorMessageAndDelegatesAcknowledgement() {
        Message<String> message = intercepted();

        Message<?> converted = converter.convert(message, PubSubMessage.class);

        assertThat(converted).isInstanceOf(PubSubMessage.class);
        PubSubMessage restored = (PubSubMessage) converted;
        assertThat(restored.getPayload()).isEqualTo("hello");
        assertThat(restored.getMetadata()).isSameAs(message.getMetadata());
        assertThat(restored.getMessage()).isSameAs(pubsubMessage);

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
        assertThat(converter.canConvert(tracked, PubSubMessage.class)).isTrue();

        PubSubMessage restored = (PubSubMessage) converter.convert(tracked, PubSubMessage.class);

        // Acknowledging the restored message acknowledges the tracked message.
        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        assertThat(channel.getInFlightCount()).isZero();
    }
}
