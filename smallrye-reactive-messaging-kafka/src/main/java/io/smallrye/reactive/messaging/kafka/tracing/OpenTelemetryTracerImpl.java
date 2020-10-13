package io.smallrye.reactive.messaging.kafka.tracing;

import java.util.Optional;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.grpc.Context;
import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.context.Scope;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.Tracer;
import io.opentelemetry.trace.TracingContextUtils;
import io.opentelemetry.trace.attributes.SemanticAttributes;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class OpenTelemetryTracerImpl implements OpenTelemetryTracer {
    public static final Tracer TRACER = OpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging.kafka");

    public OpenTelemetryTracerImpl() {
    }

    public void createOutgoingTrace(Message<?> message, String topic, int partition, Headers headers) {
        Optional<TracingMetadata> tracingMetadata = TracingMetadata.fromMessage(message);

        final Span.Builder spanBuilder = TRACER.spanBuilder(topic + " send")
                .setSpanKind(Span.Kind.PRODUCER);

        if (tracingMetadata.isPresent()) {
            // Handle possible parent span
            final Context parentSpanContext = tracingMetadata.get().getPreviousContext();
            if (parentSpanContext != null) {
                spanBuilder.setParent(parentSpanContext);
            } else {
                spanBuilder.setNoParent();
            }

            // Handle possible adjacent spans
            final SpanContext incomingSpan = tracingMetadata.get().getCurrentSpanContext();
            if (incomingSpan != null && incomingSpan.isValid()) {
                spanBuilder.addLink(incomingSpan);
            }
        } else {
            spanBuilder.setNoParent();
        }

        final Span span = spanBuilder.startSpan();
        Scope scope = TracingContextUtils.currentContextWith(span);

        // Set Span attributes
        span.setAttribute("partition", partition);
        span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "kafka");
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, topic);
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "topic");

        // Set span onto headers
        OpenTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), headers, HeaderInjectAdapter.SETTER);
        span.end();
        scope.close();
    }

    public <K, V> void createIncomingTrace(IncomingKafkaRecord<K, V> kafkaRecord) {
        TracingMetadata tracingMetadata = TracingMetadata.fromMessage(kafkaRecord).orElse(TracingMetadata.empty());

        final Span.Builder spanBuilder = TRACER.spanBuilder(kafkaRecord.getTopic() + " receive")
                .setSpanKind(Span.Kind.CONSUMER);

        // Handle possible parent span
        final Context parentSpanContext = tracingMetadata.getPreviousContext();
        if (parentSpanContext != null) {
            spanBuilder.setParent(parentSpanContext);
        } else {
            spanBuilder.setNoParent();
        }

        final Span span = spanBuilder.startSpan();

        // Set Span attributes
        span.setAttribute("partition", kafkaRecord.getPartition());
        span.setAttribute("offset", kafkaRecord.getOffset());
        span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "kafka");
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, kafkaRecord.getTopic());
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "topic");

        kafkaRecord.injectTracingMetadata(tracingMetadata.withSpan(span));

        span.end();
    }

    @Override
    public <K, T> TracingMetadata extractTracingMetadata(IncomingKafkaRecordMetadata<K, T> kafkaMetadata,
            KafkaConsumerRecord<K, T> record) {
        TracingMetadata tracingMetadata = TracingMetadata.empty();
        if (record.headers() != null) {
            // Read tracing headers
            Context context = OpenTelemetry.getPropagators().getTextMapPropagator()
                    .extract(Context.current(), kafkaMetadata.getHeaders(), HeaderExtractAdapter.GETTER);
            tracingMetadata = TracingMetadata.withPrevious(context);
        }

        return tracingMetadata;
    }
}
