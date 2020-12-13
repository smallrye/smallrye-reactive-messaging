package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubMessages.msg;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubMessage implements Message<String> {

    private final PubsubMessage message;

    private final AckReplyConsumer ackReplyConsumer;

    public PubSubMessage(final PubsubMessage message) {
        this.message = Objects.requireNonNull(message, msg.isRequired("message"));
        this.ackReplyConsumer = null;
    }

    public PubSubMessage(final PubsubMessage message, final AckReplyConsumer ackReplyConsumer) {
        this.message = Objects.requireNonNull(message, msg.isRequired("message"));
        this.ackReplyConsumer = Objects.requireNonNull(ackReplyConsumer, msg.isRequired("ackReplyConsumer"));
    }

    public PubsubMessage getMessage() {
        return message;
    }

    @Override
    public String getPayload() {
        return message.getData().toStringUtf8();
    }

    @Override
    public CompletionStage<Void> ack() {
        if (ackReplyConsumer != null) {
            ackReplyConsumer.ack();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
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
