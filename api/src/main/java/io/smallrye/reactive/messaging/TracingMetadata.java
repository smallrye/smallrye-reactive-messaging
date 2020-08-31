package io.smallrye.reactive.messaging;

import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanContext;
import io.smallrye.common.annotation.Experimental;

@Experimental("Tracer metadata is a SmallRye specific feature for integrating with OpenTelemetry")
public class TracingMetadata {
    private static final TracingMetadata EMPTY = new TracingMetadata(null);

    private final SpanContext currentSpanContext;
    private final SpanContext previousSpanContext;

    private TracingMetadata(SpanContext spanContext) {
        this(spanContext, null);
    }

    private TracingMetadata(SpanContext spanContext, SpanContext previousSpanContext) {
        this.currentSpanContext = spanContext;
        this.previousSpanContext = previousSpanContext;
    }

    /**
     * Returns an empty tracing metadata.
     *
     * @return the empty instance
     */
    public static TracingMetadata empty() {
        return EMPTY;
    }

    /**
     * Retrieves the tracing metadata from inside the {@link Metadata} of a {@link Message}.
     *
     * @param message message containing metadata, must not be {@code null}.
     * @return an {@link Optional} containing the attached {@link TracingMetadata},
     *         empty if none.
     */
    public static Optional<TracingMetadata> fromMessage(Message<?> message) {
        return message.getMetadata(TracingMetadata.class);
    }

    public static TracingMetadata withPrevious(Span span) {
        if (span != null) {
            return new TracingMetadata(null, span.getContext());
        }
        return EMPTY;
    }

    public TracingMetadata withSpan(Span span) {
        if (span != null) {
            return new TracingMetadata(span.getContext(), previousSpanContext);
        }
        return this;
    }

    public SpanContext getCurrentSpanContext() {
        return currentSpanContext;
    }

    public SpanContext getPreviousSpanContext() {
        return previousSpanContext;
    }
}
