package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubLogging.log;
import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubMessages.msg;

import java.util.HashMap;
import java.util.Objects;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.gcp.pubsub.tracing.PubSubOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.gcp.pubsub.tracing.PubSubTrace;

public class PubSubMessageReceiver implements MessageReceiver {

    private final MultiEmitter<? super Message<?>> emitter;
    private final PubSubOpenTelemetryInstrumenter incomingInstrumenter;
    private final PubSubConfig config;

    public PubSubMessageReceiver(MultiEmitter<? super Message<?>> emitter) {
        this(emitter, null, null);
    }

    public PubSubMessageReceiver(MultiEmitter<? super Message<?>> emitter,
            PubSubOpenTelemetryInstrumenter incomingInstrumenter, PubSubConfig config) {
        this.emitter = Objects.requireNonNull(emitter, msg.isRequired("emitter"));
        this.incomingInstrumenter = incomingInstrumenter;
        this.config = config;
    }

    @Override
    public void receiveMessage(final PubsubMessage message, final AckReplyConsumer ackReplyConsumer) {
        log.receivedMessage(message);
        Message<?> msg = new PubSubMessage(message, ackReplyConsumer);
        if (incomingInstrumenter != null && config != null) {
            PubSubTrace trace = PubSubTrace.traceSubscription(
                    config.getTopic(),
                    config.getSubscription(),
                    new HashMap<>(message.getAttributesMap()));
            msg = incomingInstrumenter.traceIncoming(msg, trace);
        }
        emitter.emit(msg);
    }

}
