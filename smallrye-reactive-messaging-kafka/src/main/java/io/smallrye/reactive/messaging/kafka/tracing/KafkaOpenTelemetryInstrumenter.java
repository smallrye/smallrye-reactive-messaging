package io.smallrye.reactive.messaging.kafka.tracing;

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
public class KafkaOpenTelemetryInstrumenter {

    private final Instrumenter<KafkaTrace, Void> instrumenter;

    private KafkaOpenTelemetryInstrumenter(Instrumenter<KafkaTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static KafkaOpenTelemetryInstrumenter createForSource(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), true);
    }

    public static KafkaOpenTelemetryInstrumenter createForSink(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), false);
    }

    private static KafkaOpenTelemetryInstrumenter create(OpenTelemetry openTelemetry, boolean source) {

        MessageOperation messageOperation = source ? MessageOperation.RECEIVE : MessageOperation.PUBLISH;

        KafkaAttributesExtractor kafkaAttributesExtractor = new KafkaAttributesExtractor();
        MessagingAttributesGetter<KafkaTrace, Void> messagingAttributesGetter = kafkaAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<KafkaTrace, Void> builder = Instrumenter.builder(openTelemetry,
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, messageOperation));

        builder
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, messageOperation))
                .addAttributesExtractor(kafkaAttributesExtractor);

        Instrumenter<KafkaTrace, Void> instrumenter;
        if (source) {
            instrumenter = builder.buildConsumerInstrumenter(KafkaTraceTextMapGetter.INSTANCE);
        } else {
            instrumenter = builder.buildProducerInstrumenter(KafkaTraceTextMapSetter.INSTANCE);
        }

        return new KafkaOpenTelemetryInstrumenter(instrumenter);
    }

    public Message<?> traceIncoming(Message<?> kafkaRecord, KafkaTrace kafkaTrace, boolean makeCurrent) {
        return TracingUtils.traceIncoming(instrumenter, kafkaRecord, kafkaTrace, makeCurrent);
    }

    public void traceOutgoing(Message<?> message, KafkaTrace kafkaTrace) {
        TracingUtils.traceOutgoing(instrumenter, message, kafkaTrace);
    }
}
