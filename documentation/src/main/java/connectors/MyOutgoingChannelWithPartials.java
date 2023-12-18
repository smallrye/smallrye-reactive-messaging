package connectors;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import connectors.api.BrokerClient;
import connectors.api.SendMessage;
import connectors.tracing.MyOpenTelemetryInstrumenter;
import connectors.tracing.MyTrace;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.helpers.SenderProcessor;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.mutiny.core.Vertx;

public class MyOutgoingChannelWithPartials {
    private SenderProcessor processor;
    private Flow.Subscriber<? extends Message<?>> subscriber;
    private final BrokerClient client;
    private final String topic;
    private final boolean tracingEnabled;

    private Instrumenter<MyTrace, Void> instrumenter;

    public MyOutgoingChannelWithPartials(Vertx vertx, MyConnectorOutgoingConfiguration oc, BrokerClient client) {
        this.client = client;
        this.topic = oc.getTopic().orElse(oc.getChannel());
        this.tracingEnabled = true;
        // <sender-processor>
        long requests = oc.getMaxPendingMessages();
        boolean waitForWriteCompletion = oc.getWaitForWriteCompletion();
        if (requests <= 0) {
            requests = Long.MAX_VALUE;
        }
        this.processor = new SenderProcessor(requests, waitForWriteCompletion, m -> publishMessage(client, m));
        this.subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(f -> {
            // log the failure
        }));
        // </sender-processor>
        instrumenter = MyOpenTelemetryInstrumenter.createInstrumenter(false);

    }

    // <send-message>
    private Uni<Void> publishMessage(BrokerClient client, Message<?> message) {
        // construct the outgoing message
        SendMessage sendMessage;
        Object payload = message.getPayload();
        if (payload instanceof SendMessage) {
            sendMessage = (SendMessage) message.getPayload();
        } else {
            sendMessage = new SendMessage();
            sendMessage.setPayload(payload);
            sendMessage.setTopic(topic);
            message.getMetadata(MyOutgoingMetadata.class).ifPresent(out -> {
                sendMessage.setTopic(out.getTopic());
                sendMessage.setKey(out.getKey());
                //...
            });
        }
        return Uni.createFrom().completionStage(() -> client.send(sendMessage))
                .onItem().transformToUni(receipt -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(message.nack(t)));
    }
    // </send-message>

    // <outgoing-tracing>
    private Uni<Void> publishMessageWithTracing(BrokerClient client, Message<?> message) {
        // construct the outgoing message
        SendMessage sendMessage;
        Object payload = message.getPayload();
        if (payload instanceof SendMessage) {
            sendMessage = (SendMessage) message.getPayload();
        } else {
            sendMessage = new SendMessage();
            sendMessage.setPayload(payload);
            sendMessage.setTopic(topic);
            message.getMetadata(MyOutgoingMetadata.class).ifPresent(out -> {
                sendMessage.setTopic(out.getTopic());
                sendMessage.setKey(out.getKey());
                //...
            });
        }
        if (tracingEnabled) {
            Map<String, String> properties = new HashMap<>();
            TracingUtils.traceOutgoing(instrumenter, message, new MyTrace.Builder()
                    .withProperties(properties)
                    .withTopic(sendMessage.getTopic())
                    .build());
            sendMessage.setProperties(properties);
        }
        return Uni.createFrom().completionStage(() -> client.send(sendMessage))
                .onItem().transformToUni(receipt -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(message.nack(t)));
    }
    // </outgoing-tracing>

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return this.subscriber;
    }
}
