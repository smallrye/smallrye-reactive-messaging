package io.smallrye.reactive.messaging.gcp.pubsub;

import java.util.Objects;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

import io.smallrye.mutiny.subscription.MultiEmitter;

public class PubSubMessageReceiver implements MessageReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageReceiver.class);

    private final MultiEmitter<? super Message<?>> emitter;

    public PubSubMessageReceiver(MultiEmitter<? super Message<?>> emitter) {
        this.emitter = Objects.requireNonNull(emitter, "emitter is required");
    }

    @Override
    public void receiveMessage(final PubsubMessage message, final AckReplyConsumer ackReplyConsumer) {
        LOGGER.trace("Received pub/sub message {}", message);
        emitter.emit(new PubSubMessage(message, ackReplyConsumer));
    }

}
