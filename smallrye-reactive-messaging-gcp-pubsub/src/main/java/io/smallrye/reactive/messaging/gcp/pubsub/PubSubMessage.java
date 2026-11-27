package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubMessages.msg;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubMessage implements Message<String> {

    private final PubsubMessage message;

    private final AckReplyConsumer ackReplyConsumer;

    private final Metadata metadata;

    public PubSubMessage(final PubsubMessage message) {
        this.message = Objects.requireNonNull(message, msg.isRequired("message"));
        this.ackReplyConsumer = null;
        // A message to send: the outbound connector reads its attributes and ordering key from the metadata.
        this.metadata = Metadata.of(new OutgoingPubSubMetadata(message.getAttributesMap(),
                message.getOrderingKey().isEmpty() ? null : message.getOrderingKey()));
    }

    public PubSubMessage(final PubsubMessage message, final AckReplyConsumer ackReplyConsumer) {
        this.message = Objects.requireNonNull(message, msg.isRequired("message"));
        this.ackReplyConsumer = Objects.requireNonNull(ackReplyConsumer, msg.isRequired("ackReplyConsumer"));
        // A received message: its metadata lets the outbound connector and the type converter find it.
        this.metadata = Metadata.of(new IncomingPubSubMetadata(message));
    }

    public PubsubMessage getMessage() {
        return message;
    }

    @Override
    public String getPayload() {
        return message.getData().toStringUtf8();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        if (ackReplyConsumer != null) {
            ackReplyConsumer.ack();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        return this::ack;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PubSubMessage that = (PubSubMessage) o;
        return Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message);
    }

    @Override
    public String toString() {
        return "PubSubMessage[" +
                "message=" + message +
                ']';
    }
}
