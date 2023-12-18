package ${package};

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import ${package}.api.BrokerClient;
import ${package}.api.SendMessage;
import ${package}.tracing.${connectorPrefix}OpenTelemetryInstrumenter;
import ${package}.tracing.${connectorPrefix}Trace;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.helpers.SenderProcessor;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.mutiny.core.Vertx;

public class ${connectorPrefix}OutgoingChannel {

    private final String channel;
    private final Flow.Subscriber<? extends Message<?>> subscriber;
    private final SenderProcessor processor;
    private final BrokerClient client;
    private final String topic;
    private final boolean tracingEnabled;

    private final Instrumenter<${connectorPrefix}Trace, Void> instrumenter;

    public ${connectorPrefix}OutgoingChannel(Vertx vertx, ${connectorPrefix}ConnectorOutgoingConfiguration oc, BrokerClient client) {
        this.channel = oc.getChannel();
        this.client = client;
        this.topic = oc.getTopic().orElse(oc.getChannel());
        this.tracingEnabled = oc.getTracingEnabled();
        if (tracingEnabled) {
            this.instrumenter = ${connectorPrefix}OpenTelemetryInstrumenter.createInstrumenter(false);
        } else {
            this.instrumenter = null;
        }
        this.processor = new SenderProcessor(oc.getMaxPendingMessages(), oc.getWaitForWriteCompletion(), m -> publishMessage(client, m));
        this.subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(f -> {
            // log the failure
        }));
    }

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
            message.getMetadata(${connectorPrefix}OutgoingMetadata.class).ifPresent(out -> {
                sendMessage.setTopic(out.getTopic());
                sendMessage.setKey(out.getKey());
                //...
            });
        }
        if (tracingEnabled) {
            Map<String, String> properties = new HashMap<>();
            TracingUtils.traceOutgoing(instrumenter, message, new ${connectorPrefix}Trace.Builder()
                    .withProperties(properties)
                    .withTopic(sendMessage.getTopic())
                    .build());
            sendMessage.setProperties(properties);
        }
        return Uni.createFrom().completionStage(() -> client.send(sendMessage))
                .onItem().transformToUni(receipt -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(message.nack(t)));
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return this.subscriber;
    }

    public String getChannel() {
        return this.channel;
    }
}
