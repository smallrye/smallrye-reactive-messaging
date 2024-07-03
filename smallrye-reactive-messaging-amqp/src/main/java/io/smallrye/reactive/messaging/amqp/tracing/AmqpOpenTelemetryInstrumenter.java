package io.smallrye.reactive.messaging.amqp.tracing;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

public class AmqpOpenTelemetryInstrumenter {
    private final Instrumenter<AmqpMessage<?>, Void> instrumenter;

    private AmqpOpenTelemetryInstrumenter(Instrumenter<AmqpMessage<?>, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static AmqpOpenTelemetryInstrumenter createForConnector(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), false);
    }

    public static AmqpOpenTelemetryInstrumenter createForSender(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(TracingUtils.getOpenTelemetry(openTelemetryInstance), true);
    }

    private static AmqpOpenTelemetryInstrumenter create(OpenTelemetry openTelemetry, boolean sender) {
        MessageOperation messageOperation = sender ? MessageOperation.PUBLISH : MessageOperation.RECEIVE;
        AmqpAttributesExtractor amqpAttributesExtractor = new AmqpAttributesExtractor();
        MessagingAttributesGetter<AmqpMessage<?>, Void> messagingAttributesGetter = amqpAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<AmqpMessage<?>, Void> builder = Instrumenter.builder(openTelemetry,
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, messageOperation));

        builder
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, messageOperation))
                .addAttributesExtractor(amqpAttributesExtractor)
                .buildProducerInstrumenter(AmqpMessageTextMapSetter.INSTANCE);

        Instrumenter<AmqpMessage<?>, Void> instrumenter;
        if (sender) {
            instrumenter = builder.buildProducerInstrumenter(AmqpMessageTextMapSetter.INSTANCE);
        } else {
            instrumenter = builder.buildConsumerInstrumenter(AmqpMessageTextMapGetter.INSTANCE);
        }
        return new AmqpOpenTelemetryInstrumenter(instrumenter);
    }

    public Message<?> traceIncoming(Message<?> m, AmqpMessage<?> trace) {
        return TracingUtils.traceIncoming(instrumenter, m, (AmqpMessage<?>) trace);
    }

    public void traceOutgoing(Message<?> msg, AmqpMessage<Object> trace) {
        TracingUtils.traceOutgoing(instrumenter, msg, trace);
    }
}
