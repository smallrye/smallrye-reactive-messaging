package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.TRACER;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.tracing.HeaderInjectAdapter;

public class KafkaRecordHelper {

    public static Headers getHeaders(OutgoingKafkaRecordMetadata<?> om,
            IncomingKafkaRecordMetadata<?, ?> im,
            RuntimeKafkaSinkConfiguration configuration) {
        Headers headers = new RecordHeaders();
        // First incoming headers, so that they can be overridden by outgoing headers
        if (isNotBlank(configuration.getPropagateHeaders()) && im != null && im.getHeaders() != null) {
            Set<String> headersToPropagate = Arrays.stream(configuration.getPropagateHeaders().split(","))
                    .map(String::trim)
                    .collect(Collectors.toSet());

            for (Header header : im.getHeaders()) {
                if (headersToPropagate.contains(header.key())) {
                    headers.add(header);
                }
            }
        }
        // add outgoing metadata headers, and override incoming headers if needed
        if (om != null && om.getHeaders() != null) {
            om.getHeaders().forEach(headers::add);
        }
        return headers;
    }

    public static boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }

    public static void createOutgoingTrace(Message<?> message, String topic, Integer partition, Headers headers) {
        Optional<TracingMetadata> tracingMetadata = TracingMetadata.fromMessage(message);

        final SpanBuilder spanBuilder = TRACER.spanBuilder(topic + " send")
                .setSpanKind(SpanKind.PRODUCER);

        if (tracingMetadata.isPresent()) {
            // Handle possible parent span
            final Context parentSpanContext = tracingMetadata.get().getCurrentContext();
            if (parentSpanContext != null) {
                spanBuilder.setParent(parentSpanContext);
            } else {
                spanBuilder.setNoParent();
            }
        } else {
            spanBuilder.setNoParent();
        }

        final Span span = spanBuilder.startSpan();
        Scope scope = span.makeCurrent();

        // Set Span attributes
        if (partition != null && partition != -1) {
            span.setAttribute(SemanticAttributes.MESSAGING_KAFKA_PARTITION, partition);
        }
        span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "kafka");
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, topic);
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "topic");

        // Set span onto headers
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), headers, HeaderInjectAdapter.SETTER);
        span.end();
        scope.close();
    }

}
