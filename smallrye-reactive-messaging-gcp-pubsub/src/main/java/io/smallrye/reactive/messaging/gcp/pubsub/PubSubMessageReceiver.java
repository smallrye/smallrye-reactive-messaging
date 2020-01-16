package io.smallrye.reactive.messaging.gcp.pubsub;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;

public class PubSubMessageReceiver implements AutoCloseable, FlowableOnSubscribe<Message<?>>, MessageReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageReceiver.class);

    private FlowableEmitter<Message<?>> emitter;

    @Override
    public void receiveMessage(final PubsubMessage message, final AckReplyConsumer ackReplyConsumer) {
        LOGGER.trace("Received pub/sub message {}", message);
        if (emitter == null) {
            throw new IllegalStateException("Emitter not set on this MessageReceiver");
        }

        emitter.onNext(new PubSubMessage(message, ackReplyConsumer));
    }

    @Override
    public void subscribe(final FlowableEmitter<Message<?>> flowableEmitter) throws Exception {
        if (emitter != null) {
            throw new IllegalStateException("Emitter registered more than once on this MessageReceiver");
        }
        this.emitter = flowableEmitter;
    }

    @Override
    public void close() {
        if (emitter != null) {
            emitter.onComplete();
        }
    }
}
