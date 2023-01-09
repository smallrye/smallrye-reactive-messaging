package io.smallrye.reactive.messaging;

import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.smallrye.common.annotation.Experimental;

@Experimental("Tracer metadata is a SmallRye specific feature for integrating with OpenTelemetry")
public class TracingMetadata {
    private static final TracingMetadata EMPTY = new TracingMetadata(null);

    private final Context currentSpanContext;
    private final Context previousSpanContext;

    private TracingMetadata(Context spanContext) {
        this(spanContext, null);
    }

    private TracingMetadata(Context spanContext, Context previousSpanContext) {
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

    public static TracingMetadata withPrevious(Context previousContext) {
        if (previousContext != null) {
            return new TracingMetadata(null, previousContext);
        }
        return EMPTY;
    }

    public static TracingMetadata withCurrent(Context currentContext) {
        if (currentContext != null) {
            return new TracingMetadata(currentContext);
        }
        return EMPTY;
    }

    public static TracingMetadata with(Context currentSpanContext, Context previousSpanContext) {
        if (currentSpanContext != null || previousSpanContext != null) {
            return new TracingMetadata(currentSpanContext, previousSpanContext);
        }
        return EMPTY;
    }

    @Deprecated
    public TracingMetadata withSpan(Span span) {
        if (span != null) {
            return new TracingMetadata(Context.root().with(span), previousSpanContext);
        }
        return this;
    }

    public Context getCurrentContext() {
        return currentSpanContext;
    }

    public Context getPreviousContext() {
        return previousSpanContext;
    }
}
