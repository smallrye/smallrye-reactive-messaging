package io.smallrye.reactive.messaging.rabbitmq.tracing;

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

public class RabbitMQOpenTelemetryInstrumenter {
    private final Instrumenter<RabbitMQTrace, Void> instrumenter;

    protected RabbitMQOpenTelemetryInstrumenter(Instrumenter<RabbitMQTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static RabbitMQOpenTelemetryInstrumenter createForSender(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), true);
    }

    public static RabbitMQOpenTelemetryInstrumenter createForConnector(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), false);
    }

    private static RabbitMQOpenTelemetryInstrumenter create(OpenTelemetry openTelemetry, boolean sender) {
        MessageOperation messageOperation = sender ? MessageOperation.PUBLISH : MessageOperation.RECEIVE;

        RabbitMQTraceAttributesExtractor rabbitMQAttributesExtractor = new RabbitMQTraceAttributesExtractor();
        MessagingAttributesGetter<RabbitMQTrace, Void> messagingAttributesGetter = rabbitMQAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<RabbitMQTrace, Void> builder = Instrumenter.builder(openTelemetry,
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, messageOperation));

        builder.addAttributesExtractor(rabbitMQAttributesExtractor)
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, messageOperation));
        Instrumenter<RabbitMQTrace, Void> instrumenter;
        if (sender) {
            instrumenter = builder.buildProducerInstrumenter(RabbitMQTraceTextMapSetter.INSTANCE);
        } else {
            instrumenter = builder.buildConsumerInstrumenter(RabbitMQTraceTextMapGetter.INSTANCE);
        }
        return new RabbitMQOpenTelemetryInstrumenter(instrumenter);
    }

    public void traceOutgoing(Message<?> message, RabbitMQTrace trace) {
        TracingUtils.traceOutgoing(instrumenter, message, trace);
    }

    public Message<?> traceIncoming(Message<?> msg, RabbitMQTrace trace) {
        return TracingUtils.traceIncoming(instrumenter, msg, trace);
    }
}
