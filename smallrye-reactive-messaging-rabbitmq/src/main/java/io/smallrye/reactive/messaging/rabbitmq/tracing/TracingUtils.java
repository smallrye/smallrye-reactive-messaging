package io.smallrye.reactive.messaging.rabbitmq.tracing;

import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.reactive.messaging.TracingMetadata;

/**
 * Utility methods to manage spans for incoming and outgoing messages.
 */
public abstract class TracingUtils {
    /**
     * Private constructor to prevent instantiation.
     */
    private TracingUtils() {
    }

    public static void createOutgoingTrace(
            final Instrumenter<RabbitMQTrace, Void> instrumenter,
            final Message<?> msg,
            final Map<String, Object> headers,
            final String exchange) {

        Optional<TracingMetadata> tracingMetadata = TracingMetadata.fromMessage(msg);
        RabbitMQTrace message = RabbitMQTrace.trace(exchange, headers);

        Context parentContext = tracingMetadata.map(TracingMetadata::getCurrentContext).orElse(Context.current());
        Context spanContext;
        Scope scope = null;

        boolean shouldStart = instrumenter.shouldStart(parentContext, message);
        if (shouldStart) {
            try {
                spanContext = instrumenter.start(parentContext, message);
                scope = spanContext.makeCurrent();
                instrumenter.end(spanContext, message, null, null);
            } finally {
                if (scope != null) {
                    scope.close();
                }
            }
        }
    }
}
