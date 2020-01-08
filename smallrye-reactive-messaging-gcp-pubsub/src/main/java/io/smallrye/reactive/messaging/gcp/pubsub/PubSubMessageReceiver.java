package io.smallrye.reactive.messaging.gcp.pubsub;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;

public class PubSubMessageReceiver implements AutoCloseable, FlowableOnSubscribe<PubSubMessage>, MessageReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageReceiver.class);

    private FlowableEmitter<PubSubMessage> emitter;

    @Override
    public void close() {
        if (emitter != null) {
            emitter.onComplete();
        }
    }

    @Override
    public void receiveMessage(final PubsubMessage message, final AckReplyConsumer ackReplyConsumer) {
        LOGGER.trace("Received pub/sub message {}", message);
        if (emitter == null) {
            throw new IllegalStateException("Unable to handle message due to no emitter");
        }

        final PubSubMessage pubSubMessage = new PubSubMessage(message, ackReplyConsumer);
        emitter.onNext(pubSubMessage);
    }

    @Override
    public void subscribe(final FlowableEmitter<PubSubMessage> emitter) {
        this.emitter = Objects.requireNonNull(emitter, "emitter is required");
    }
}
