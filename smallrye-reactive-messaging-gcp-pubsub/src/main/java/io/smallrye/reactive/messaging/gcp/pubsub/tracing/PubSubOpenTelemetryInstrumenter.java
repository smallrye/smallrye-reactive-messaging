package io.smallrye.reactive.messaging.gcp.pubsub.tracing;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

public class PubSubOpenTelemetryInstrumenter {
    private final Instrumenter<PubSubTrace, Void> instrumenter;

    private PubSubOpenTelemetryInstrumenter(Instrumenter<PubSubTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static PubSubOpenTelemetryInstrumenter createForConnector(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), false);
    }

    public static PubSubOpenTelemetryInstrumenter createForSender(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), true);
    }

    private static PubSubOpenTelemetryInstrumenter create(OpenTelemetry openTelemetry, boolean sender) {
        MessageOperation messageOperation = sender ? MessageOperation.PUBLISH : MessageOperation.RECEIVE;

        PubSubAttributesExtractor pubSubAttributesExtractor = new PubSubAttributesExtractor();
        MessagingAttributesGetter<PubSubTrace, Void> messagingAttributesGetter = pubSubAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<PubSubTrace, Void> builder = Instrumenter.builder(openTelemetry,
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, messageOperation));

        builder.addAttributesExtractor(pubSubAttributesExtractor)
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, messageOperation));

        Instrumenter<PubSubTrace, Void> instrumenter;
        if (sender) {
            instrumenter = builder.buildProducerInstrumenter(PubSubTraceTextMapSetter.INSTANCE);
        } else {
            instrumenter = builder.buildConsumerInstrumenter(PubSubTraceTextMapGetter.INSTANCE);
        }
        return new PubSubOpenTelemetryInstrumenter(instrumenter);
    }

    public void traceOutgoing(Message<?> message, PubSubTrace trace) {
        TracingUtils.traceOutgoing(instrumenter, message, trace);
    }

    public Message<?> traceIncoming(Message<?> msg, PubSubTrace trace) {
        return TracingUtils.traceIncoming(instrumenter, msg, trace);
    }
}
