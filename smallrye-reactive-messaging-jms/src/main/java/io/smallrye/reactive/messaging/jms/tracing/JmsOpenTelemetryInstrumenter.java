package io.smallrye.reactive.messaging.jms.tracing;

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
public class JmsOpenTelemetryInstrumenter {

    private final Instrumenter<JmsTrace, Void> instrumenter;

    private JmsOpenTelemetryInstrumenter(Instrumenter<JmsTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static JmsOpenTelemetryInstrumenter createForSource(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), true);
    }

    public static JmsOpenTelemetryInstrumenter createForSink(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), false);
    }

    private static JmsOpenTelemetryInstrumenter create(OpenTelemetry openTelemetry, boolean source) {

        MessageOperation messageOperation = source ? MessageOperation.RECEIVE : MessageOperation.PUBLISH;

        JmsAttributesExtractor jmsAttributesExtractor = new JmsAttributesExtractor();
        MessagingAttributesGetter<JmsTrace, Void> messagingAttributesGetter = jmsAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<JmsTrace, Void> builder = Instrumenter.builder(openTelemetry,
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, messageOperation));

        builder
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, messageOperation))
                .addAttributesExtractor(jmsAttributesExtractor);

        Instrumenter<JmsTrace, Void> instrumenter;
        if (source) {
            instrumenter = builder.buildConsumerInstrumenter(JmsTraceTextMapGetter.INSTANCE);
        } else {
            instrumenter = builder.buildProducerInstrumenter(JmsTraceTextMapSetter.INSTANCE);
        }

        return new JmsOpenTelemetryInstrumenter(instrumenter);
    }

    public Message<?> traceIncoming(Message<?> message, JmsTrace jmsTrace) {
        return TracingUtils.traceIncoming(instrumenter, message, jmsTrace);
    }

    public void traceOutgoing(Message<?> message, JmsTrace jmsTrace) {
        TracingUtils.traceOutgoing(instrumenter, message, jmsTrace);
    }
}
