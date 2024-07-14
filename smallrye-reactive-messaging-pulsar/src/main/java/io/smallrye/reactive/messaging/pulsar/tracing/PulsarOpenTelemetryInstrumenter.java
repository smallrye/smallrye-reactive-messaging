package io.smallrye.reactive.messaging.pulsar.tracing;

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

public class PulsarOpenTelemetryInstrumenter {
    private final Instrumenter<PulsarTrace, Void> instrumenter;

    public PulsarOpenTelemetryInstrumenter(Instrumenter<PulsarTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static PulsarOpenTelemetryInstrumenter createForSource(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), true);
    }

    public static PulsarOpenTelemetryInstrumenter createForSink(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), false);
    }

    private static PulsarOpenTelemetryInstrumenter create(OpenTelemetry openTelemetry, boolean source) {

        MessageOperation messageOperation = source ? MessageOperation.RECEIVE : MessageOperation.PUBLISH;

        PulsarAttributesExtractor attributesExtractor = new PulsarAttributesExtractor();
        MessagingAttributesGetter<PulsarTrace, Void> messagingAttributesGetter = attributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<PulsarTrace, Void> builder = Instrumenter.builder(
                openTelemetry, "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, messageOperation));

        builder.addAttributesExtractor(MessagingAttributesExtractor.create(messagingAttributesGetter, messageOperation))
                .addAttributesExtractor(attributesExtractor);

        if (source) {
            return new PulsarOpenTelemetryInstrumenter(builder.buildConsumerInstrumenter(PulsarTraceTextMapGetter.INSTANCE));
        } else {
            return new PulsarOpenTelemetryInstrumenter(builder.buildProducerInstrumenter(PulsarTraceTextMapSetter.INSTANCE));
        }
    }

    public void traceOutgoing(Message<?> message, PulsarTrace trace) {
        TracingUtils.traceOutgoing(instrumenter, message, trace);
    }

    public void traceIncoming(Message<?> message, PulsarTrace trace) {
        TracingUtils.traceIncoming(instrumenter, message, trace);
    }
}
