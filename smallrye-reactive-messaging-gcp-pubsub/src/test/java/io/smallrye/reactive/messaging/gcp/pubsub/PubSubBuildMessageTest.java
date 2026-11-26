package io.smallrye.reactive.messaging.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator;

/**
 * The conversion of the outgoing messages, without the Pub/Sub emulator.
 */
class PubSubBuildMessageTest {

    private final PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("hello"))
            .putAttributes("key", "value")
            .setOrderingKey("ordering")
            .build();

    @Test
    void convertsAPubSubMessageToSendFromItsMetadata() {
        // Not read from the message type: a message built to be sent exposes its attributes and ordering key as
        // outgoing metadata.
        PubsubMessage converted = PubSubConnector.buildMessage(new PubSubMessage(pubsubMessage));
        assertThat(converted.getData().toStringUtf8()).isEqualTo("hello");
        assertThat(converted.getAttributesMap()).containsEntry("key", "value");
        assertThat(converted.getOrderingKey()).isEqualTo("ordering");
    }

    @Test
    void propagatesTheAttributesOfAReceivedMessageOnlyWhenAsked() {
        PubSubMessage received = new PubSubMessage(pubsubMessage, new AckReplyConsumer() {
            @Override
            public void ack() {
            }

            @Override
            public void nack() {
            }
        });

        PubsubMessage forwarded = PubSubConnector.buildMessage(received.withPayload("changed"));
        assertThat(forwarded.getData().toStringUtf8()).isEqualTo("changed");
        assertThat(forwarded.getAttributesMap()).isEmpty();
        assertThat(forwarded.getOrderingKey()).isEmpty();

        IncomingPubSubMetadata incoming = received.getMetadata(IncomingPubSubMetadata.class).orElseThrow();
        PubsubMessage propagated = PubSubConnector.buildMessage(received.withPayload("changed")
                .addMetadata(OutgoingPubSubMetadata.from(incoming).withAttribute("other", "outgoing").build()));
        assertThat(propagated.getAttributesMap()).containsEntry("key", "value").containsEntry("other", "outgoing");
        assertThat(propagated.getOrderingKey()).isEqualTo("ordering");

        PubsubMessage overridden = PubSubConnector.buildMessage(received.withPayload("changed")
                .addMetadata(OutgoingPubSubMetadata.from(incoming)
                        .withAttribute("key", "outgoing").withOrderingKey("other").build()));
        assertThat(overridden.getAttributesMap()).containsEntry("key", "outgoing");
        assertThat(overridden.getOrderingKey()).isEqualTo("other");
    }

    @Test
    void convertsATrackedMessageLikeTheMessageItWraps() {
        PausableChannelDecorator.PauserChannel channel = new PausableChannelDecorator.PauserChannel(
                new DemandPauser(), 10_000, null);
        Message<?> tracked = channel.trackMessage(Message.of("hello")
                .addMetadata(OutgoingPubSubMetadata.builder()
                        .withAttribute("key", "value").withOrderingKey("ordering").build()));

        PubsubMessage converted = PubSubConnector.buildMessage(tracked);
        assertThat(converted.getData().toStringUtf8()).isEqualTo("hello");
        assertThat(converted.getAttributesMap()).containsEntry("key", "value");
        assertThat(converted.getOrderingKey()).isEqualTo("ordering");
    }

    @Test
    void convertsOtherPayloads() {
        assertThat(PubSubConnector.buildMessage(Message.of(new PubSubMessage(pubsubMessage)))).isSameAs(pubsubMessage);
        assertThat(PubSubConnector.buildMessage(Message.of(pubsubMessage))).isSameAs(pubsubMessage);

        PubsubMessage converted = PubSubConnector.buildMessage(Message.of("text"));
        assertThat(converted.getData().toStringUtf8()).isEqualTo("text");
        assertThat(converted.getAttributesMap()).isEmpty();
    }
}
