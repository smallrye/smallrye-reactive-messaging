package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubLogging.log;
import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubMessages.msg;

import java.util.Objects;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

import io.smallrye.mutiny.subscription.MultiEmitter;

public class PubSubMessageReceiver implements MessageReceiver {

    private final MultiEmitter<? super Message<?>> emitter;

    public PubSubMessageReceiver(MultiEmitter<? super Message<?>> emitter) {
        this.emitter = Objects.requireNonNull(emitter, msg.isRequired("emitter"));
    }

    @Override
    public void receiveMessage(final PubsubMessage message, final AckReplyConsumer ackReplyConsumer) {
        log.receivedMessage(message);
        emitter.emit(new PubSubMessage(message, ackReplyConsumer));
    }

}
