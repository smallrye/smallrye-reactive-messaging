package io.smallrye.reactive.messaging.rabbitmq.tracing;

import static io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation.RECEIVE;

import java.util.List;
import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;

@ApplicationScoped
public class RabbitMQTracingSubscriberDecorator implements SubscriberDecorator {
    private final Instrumenter<RabbitMQTrace, Void> instrumenter;

    public RabbitMQTracingSubscriberDecorator() {
        RabbitMQTraceAttributesExtractor rabbitMQAttributesExtractor = new RabbitMQTraceAttributesExtractor();
        MessagingAttributesGetter<RabbitMQTrace, Void> messagingAttributesGetter = rabbitMQAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<RabbitMQTrace, Void> builder = Instrumenter.builder(GlobalOpenTelemetry.get(),
                "io.smallrye.reactive.messaging", MessagingSpanNameExtractor.create(messagingAttributesGetter, RECEIVE));

        instrumenter = builder.addAttributesExtractor(rabbitMQAttributesExtractor)
                .addAttributesExtractor(MessagingAttributesExtractor.create(messagingAttributesGetter, RECEIVE))
                .buildConsumerInstrumenter(RabbitMQTraceTextMapGetter.INSTANCE);
    }

    @Override
    public Multi<? extends Message<?>> decorate(
            final Multi<? extends Message<?>> toBeSubscribed,
            final List<String> channelName,
            final boolean isConnector) {
        return toBeSubscribed.onItem().transform(this::traceMessage);
    }

    private Message<?> traceMessage(final Message<?> message) {
        Optional<IncomingRabbitMQMetadata> incomingRabbitMQMetadata = message.getMetadata().get(IncomingRabbitMQMetadata.class);
        if (incomingRabbitMQMetadata.isEmpty()) {
            return message;
        }

        IncomingRabbitMQMetadata metadata = incomingRabbitMQMetadata.get();
        if (!metadata.isTracingEnabled()) {
            // TODO - We can optimize this and not even create the bean?
            return message;
        }

        TracingMetadata tracingMetadata = TracingMetadata.fromMessage(message).orElse(TracingMetadata.empty());
        RabbitMQTrace trace = RabbitMQTrace.trace(metadata.getQueueName(), metadata.getHeaders());

        Context parentContext = tracingMetadata.getPreviousContext();
        if (parentContext == null) {
            parentContext = Context.current();
        }
        Context spanContext;
        Scope scope = null;

        boolean shouldStart = instrumenter.shouldStart(parentContext, trace);
        if (shouldStart) {
            try {
                spanContext = instrumenter.start(parentContext, trace);
                scope = spanContext.makeCurrent();
                instrumenter.end(spanContext, trace, null, null);
                return message.addMetadata(TracingMetadata.with(spanContext, parentContext));
            } finally {
                if (scope != null) {
                    scope.close();
                }
            }
        }

        return message;
    }
}
