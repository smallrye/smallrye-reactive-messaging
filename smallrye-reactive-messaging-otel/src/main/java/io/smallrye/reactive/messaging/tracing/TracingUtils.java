package io.smallrye.reactive.messaging.tracing;

import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;

public class TracingUtils {

    private TracingUtils() {
    }

    public static <T> void traceOutgoing(Instrumenter<T, Void> instrumenter, Message<?> message, T trace) {
        Optional<TracingMetadata> tracingMetadata = TracingMetadata.fromMessage(message);

        Context parentContext = tracingMetadata.map(TracingMetadata::getCurrentContext).orElse(Context.current());
        Context spanContext;
        Scope scope = null;

        boolean shouldStart = instrumenter.shouldStart(parentContext, trace);
        if (shouldStart) {
            try {
                spanContext = instrumenter.start(parentContext, trace);
                scope = spanContext.makeCurrent();
                instrumenter.end(spanContext, trace, null, null);
            } finally {
                if (scope != null) {
                    scope.close();
                }
            }
        }
    }

    public static <T> Message<?> traceIncoming(Instrumenter<T, Void> instrumenter, Message<?> msg, T trace) {
        return traceIncoming(instrumenter, msg, trace, true);
    }

    public static <T> Message<?> traceIncoming(Instrumenter<T, Void> instrumenter, Message<?> msg, T trace,
            boolean makeCurrent) {
        TracingMetadata tracingMetadata = TracingMetadata.fromMessage(msg).orElse(TracingMetadata.empty());
        Context parentContext = tracingMetadata.getPreviousContext();
        if (parentContext == null) {
            parentContext = Context.current();
        }
        Context spanContext;
        Scope scope = null;
        boolean shouldStart = instrumenter.shouldStart(parentContext, trace);

        if (shouldStart) {
            spanContext = instrumenter.start(parentContext, trace);
            if (makeCurrent) {
                scope = spanContext.makeCurrent();
            }

            Message<?> message;
            TracingMetadata newTracingMetadata = TracingMetadata.with(spanContext, parentContext);
            if (msg instanceof MetadataInjectableMessage) {
                ((MetadataInjectableMessage<?>) msg).injectMetadata(newTracingMetadata);
                message = msg;
            } else {
                message = msg.addMetadata(newTracingMetadata);
            }

            try {
                instrumenter.end(spanContext, trace, null, null);
            } finally {
                if (scope != null) {
                    scope.close();
                }
            }
            return message;
        }
        return msg;
    }
}
