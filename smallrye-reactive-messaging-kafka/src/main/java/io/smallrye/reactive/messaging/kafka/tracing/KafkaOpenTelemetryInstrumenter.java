package io.smallrye.reactive.messaging.kafka.tracing;

import java.util.Optional;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

/**
 * Encapsulates the OpenTelemetry instrumentation API so that those classes are only needed if
 * users explicitly enable tracing.
 */
public class KafkaOpenTelemetryInstrumenter {

    private final Instrumenter<KafkaTrace, Void> consumerInstrumenter;
    private final Instrumenter<KafkaTrace, RecordMetadata> producerInstrumenter;

    private KafkaOpenTelemetryInstrumenter(
            Instrumenter<KafkaTrace, Void> consumerInstrumenter,
            Instrumenter<KafkaTrace, RecordMetadata> producerInstrumenter) {
        this.consumerInstrumenter = consumerInstrumenter;
        this.producerInstrumenter = producerInstrumenter;
    }

    public static KafkaOpenTelemetryInstrumenter createForSource(Instance<OpenTelemetry> openTelemetryInstance) {
        return createConsumer(TracingUtils.getOpenTelemetry(openTelemetryInstance));
    }

    public static KafkaOpenTelemetryInstrumenter createForSink(Instance<OpenTelemetry> openTelemetryInstance) {
        return createProducer(TracingUtils.getOpenTelemetry(openTelemetryInstance));
    }

    private static KafkaOpenTelemetryInstrumenter createConsumer(OpenTelemetry openTelemetry) {
        KafkaAttributesExtractor kafkaAttributesExtractor = new KafkaAttributesExtractor();
        MessagingAttributesGetter<KafkaTrace, Void> messagingAttributesGetter = kafkaAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<KafkaTrace, Void> builder = Instrumenter.builder(openTelemetry,
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, MessageOperation.RECEIVE));
        builder
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, MessageOperation.RECEIVE))
                .addAttributesExtractor(kafkaAttributesExtractor);

        Instrumenter<KafkaTrace, Void> instrumenter = builder.buildConsumerInstrumenter(KafkaTraceTextMapGetter.INSTANCE);
        return new KafkaOpenTelemetryInstrumenter(instrumenter, null);
    }

    private static KafkaOpenTelemetryInstrumenter createProducer(OpenTelemetry openTelemetry) {
        KafkaProducerAttributesExtractor kafkaAttributesExtractor = new KafkaProducerAttributesExtractor();
        MessagingAttributesGetter<KafkaTrace, RecordMetadata> messagingAttributesGetter = kafkaAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<KafkaTrace, RecordMetadata> builder = Instrumenter.builder(openTelemetry,
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, MessageOperation.PUBLISH));
        builder
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, MessageOperation.PUBLISH))
                .addAttributesExtractor(kafkaAttributesExtractor);

        Instrumenter<KafkaTrace, RecordMetadata> instrumenter = builder
                .buildProducerInstrumenter(KafkaTraceTextMapSetter.INSTANCE);
        return new KafkaOpenTelemetryInstrumenter(null, instrumenter);
    }

    public Message<?> traceIncoming(Message<?> kafkaRecord, KafkaTrace kafkaTrace, boolean makeCurrent) {
        return TracingUtils.traceIncoming(consumerInstrumenter, kafkaRecord, kafkaTrace, makeCurrent);
    }

    public Context startOutgoing(Message<?> message, KafkaTrace kafkaTrace) {
        Optional<TracingMetadata> tracingMetadata = TracingMetadata.fromMessage(message);
        Context parentContext = tracingMetadata.map(TracingMetadata::getCurrentContext).orElse(Context.current());
        if (producerInstrumenter.shouldStart(parentContext, kafkaTrace)) {
            Context spanContext = producerInstrumenter.start(parentContext, kafkaTrace);
            Scope scope = spanContext.makeCurrent();
            scope.close();
            return spanContext;
        }
        return null;
    }

    public void endOutgoing(Context spanContext, KafkaTrace kafkaTrace, RecordMetadata metadata, Throwable error) {
        if (spanContext != null) {
            producerInstrumenter.end(spanContext, kafkaTrace, metadata, error);
        }
    }
}
