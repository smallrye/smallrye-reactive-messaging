package io.smallrye.reactive.messaging.rabbitmq.tracing;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.rabbitmq.client.Envelope;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

/**
 * Utility methods to manage spans for incoming and outgoing messages.
 */
public abstract class TracingUtils {

    private static final String SYSTEM_RABBITMQ = "rabbitmq";
    private static final String DESTINATION_EXCHANGE = "exchange";
    private static final String DESTINATION_QUEUE = "queue";

    // A shared tracer for use throughout the connector
    private static Tracer tracer;

    public static void initialise() {
        tracer = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging.rabbitmq");
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private TracingUtils() {
    }

    /**
     * Extract tracing metadata from a received RabbitMQ message and return
     * it as {@link TracingMetadata}.
     *
     * @param msg the incoming RabbitMQ message
     * @return a {@link TracingMetadata} instance, possibly empty but never null
     */
    public static TracingMetadata getTracingMetaData(
            final io.vertx.rabbitmq.RabbitMQMessage msg) {
        // Default
        TracingMetadata tracingMetadata = TracingMetadata.empty();

        if (msg.properties().getHeaders() != null) {
            // Extract the tracing headers from the incoming RabbitMQ message into a tracing context
            final Context context = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .extract(io.opentelemetry.context.Context.root(), msg.properties().getHeaders(),
                            HeadersMapExtractAdapter.GETTER);

            // create tracing metadata based on that tracing context
            tracingMetadata = TracingMetadata.withPrevious(context);
        }

        return tracingMetadata;
    }

    /**
     * Creates a span based on any tracing metadata in the incoming message.
     *
     * @param msg the incoming message
     * @param attributeHeaders a list (possibly empty) of header names whose values (if present)
     *        should be used as span attributes
     * @param <T> the message body type
     * @return the message, with injected tracing metadata
     */
    public static <T> Message<T> addIncomingTrace(
            final IncomingRabbitMQMessage<T> msg,
            final String queue,
            final List<String> attributeHeaders) {
        final TracingMetadata tracingMetadata = TracingMetadata.fromMessage(msg).orElse(TracingMetadata.empty());
        final Envelope envelope = msg.getRabbitMQMessage().envelope();

        final SpanBuilder spanBuilder = tracer.spanBuilder(envelope.getExchange() + " receive")
                .setSpanKind(SpanKind.CONSUMER);

        // Handle possible parent span
        final Context parentSpanContext = tracingMetadata.getPreviousContext();
        if (parentSpanContext != null) {
            spanBuilder.setParent(parentSpanContext);
        } else {
            spanBuilder.setNoParent();
        }

        final Span span = spanBuilder.startSpan();

        // Filter the incoming message headers to just those that should map to span tags
        final Map<String, Object> headerValues = msg.getHeaders().entrySet().stream()
                .filter(e -> attributeHeaders.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        try {
            setIncomingSpanAttributes(span, queue, envelope.getRoutingKey(), headerValues);

            // Make available as parent for subsequent spans inside message processing
            span.makeCurrent();

            msg.injectTracingMetadata(tracingMetadata.withSpan(span));
        } finally {
            span.end();
        }

        return msg;
    }

    /**
     * Adds "standard" RabbitMQ semantic tracing attributes to the span associated with an incoming message
     *
     * @param span the span
     * @param queue the source queue
     * @param routingKey the routing key
     * @param attributeHeaders a map (possibly empty, but never null) of headers and their values that should
     *        be mapped to span attributes. Only non-null values of type Long, String, Boolean or Double will be mapped.
     */
    private static void setIncomingSpanAttributes(
            final Span span,
            final String queue,
            final String routingKey,
            final Map<String, Object> attributeHeaders) {
        span.setAttribute(SemanticAttributes.MESSAGING_RABBITMQ_ROUTING_KEY, routingKey);
        span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, SYSTEM_RABBITMQ);
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, queue);
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, DESTINATION_QUEUE);
        setHeaderSpanAttributes(span, attributeHeaders);
    }

    /**
     * Adds "standard" RabbitMQ semantic tracing attributes to the span associated with an outgoing message
     *
     * @param span the span
     * @param exchange the destination exchange
     * @param routingKey the routing key
     * @param attributeHeaders a map (possibly empty, but never null) of headers and their values that should
     *        be mapped to span attributes. Only non-null values of type Long, String, Boolean or Double will be mapped.
     */
    private static void setOutgoingSpanAttributes(
            final Span span,
            final String exchange,
            final String routingKey,
            final Map<String, Object> attributeHeaders) {
        span.setAttribute(SemanticAttributes.MESSAGING_RABBITMQ_ROUTING_KEY, routingKey);
        span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, SYSTEM_RABBITMQ);
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, exchange);
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, DESTINATION_EXCHANGE);
        setHeaderSpanAttributes(span, attributeHeaders);
    }

    /**
     * Adds RabbitMQ header-sourced tracing attributes to the span.
     *
     * @param span the span
     * @param attributeHeaders a map (possibly empty, but never null) of headers and their values that should
     *        be mapped to span attributes. Only non-null values of type Long, String, Boolean or Double will be mapped.
     */
    private static void setHeaderSpanAttributes(final Span span, final Map<String, Object> attributeHeaders) {
        // For any of the specified headers that have a non-null value of a type
        // supported by opentelemetry, create a span attribute for that header that contains the value
        attributeHeaders.forEach((header, value) -> {
            if (value instanceof Long) {
                span.setAttribute(header, (long) value);
            } else if (value instanceof String) {
                span.setAttribute(header, (String) value);
            } else if (value instanceof Boolean) {
                span.setAttribute(header, (Boolean) value);
            } else if (value instanceof Double) {
                span.setAttribute(header, (Double) value);
            }
        });
    }

    /**
     * Creates a new outgoing message span message, and ensures span metadata is added to the
     * message headers.
     *
     * @param message the source message
     * @param headers the outgoing headers, must be mutable
     * @param exchange the target exchange
     * @param routingKey the routing key
     * @param attributeHeaders a list (possibly empty) of header names whose values (if present)
     *        should be used as span attributes
     */
    public static void createOutgoingTrace(
            final Message<?> message,
            final Map<String, Object> headers,
            final String exchange,
            final String routingKey,
            final List<String> attributeHeaders) {
        // Extract tracing metadata from the source message itself
        final TracingMetadata tracingMetadata = TracingMetadata.fromMessage(message).orElse(null);

        final SpanBuilder spanBuilder = tracer.spanBuilder(exchange + " send")
                .setSpanKind(SpanKind.PRODUCER);

        if (tracingMetadata != null) {
            // Handle possible parent span
            final Context parentSpanContext = tracingMetadata.getPreviousContext();

            if (parentSpanContext != null) {
                spanBuilder.setParent(parentSpanContext);
            } else {
                spanBuilder.setNoParent();
            }
        } else {
            spanBuilder.setNoParent();
        }

        final Span span = spanBuilder.startSpan();
        final Context sendingContext = Context.current().with(span);

        // Filter the outgoing message headers to just those that should map to span tags
        final Map<String, Object> headerValues = message.getMetadata(OutgoingRabbitMQMetadata.class)
                .map(md -> md.getHeaders().entrySet().stream()
                        .filter(e -> attributeHeaders.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .orElse(Collections.emptyMap());

        setOutgoingSpanAttributes(span, exchange, routingKey, headerValues);

        // Set span onto outgoing headers
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .inject(sendingContext, headers, HeadersMapInjectAdapter.SETTER);
        span.end();
    }
}
