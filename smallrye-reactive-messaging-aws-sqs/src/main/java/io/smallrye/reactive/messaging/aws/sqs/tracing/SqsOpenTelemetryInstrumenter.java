package io.smallrye.reactive.messaging.aws.sqs.tracing;

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

/**
 * Encapsulates the OpenTelemetry instrumentation API so that those classes are only needed if
 * users explicitly enable tracing.
 */
public class SqsOpenTelemetryInstrumenter {

    private final Instrumenter<SqsTrace, Void> instrumenter;

    private SqsOpenTelemetryInstrumenter(Instrumenter<SqsTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static SqsOpenTelemetryInstrumenter createForSource(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), true);
    }

    public static SqsOpenTelemetryInstrumenter createForSink(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), false);
    }

    private static SqsOpenTelemetryInstrumenter create(OpenTelemetry openTelemetry, boolean source) {

        MessageOperation messageOperation = source ? MessageOperation.RECEIVE : MessageOperation.PUBLISH;

        SqsAttributesExtractor sqsAttributesExtractor = new SqsAttributesExtractor();
        MessagingAttributesGetter<SqsTrace, Void> messagingAttributesGetter = sqsAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<SqsTrace, Void> builder = Instrumenter.builder(openTelemetry,
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, messageOperation));

        builder
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, messageOperation))
                .addAttributesExtractor(sqsAttributesExtractor);

        Instrumenter<SqsTrace, Void> instrumenter;
        if (source) {
            instrumenter = builder.buildConsumerInstrumenter(SqsTraceTextMapGetter.INSTANCE);
        } else {
            instrumenter = builder.buildProducerInstrumenter(SqsTraceTextMapSetter.INSTANCE);
        }

        return new SqsOpenTelemetryInstrumenter(instrumenter);
    }

    public Message<?> traceIncoming(Message<?> message, SqsTrace sqsTrace) {
        return TracingUtils.traceIncoming(instrumenter, message, sqsTrace);
    }

    public void traceOutgoing(Message<?> message, SqsTrace sqsTrace) {
        TracingUtils.traceOutgoing(instrumenter, message, sqsTrace);
    }
}
