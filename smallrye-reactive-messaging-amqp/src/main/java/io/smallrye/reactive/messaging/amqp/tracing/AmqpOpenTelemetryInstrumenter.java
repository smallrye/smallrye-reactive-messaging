package io.smallrye.reactive.messaging.amqp.tracing;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

public class AmqpOpenTelemetryInstrumenter {
    private final Instrumenter<AmqpMessage<?>, Void> instrumenter;

    private AmqpOpenTelemetryInstrumenter(Instrumenter<AmqpMessage<?>, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static AmqpOpenTelemetryInstrumenter createForConnector() {
        return create(false);
    }

    public static AmqpOpenTelemetryInstrumenter createForSender() {
        return create(true);
    }

    private static AmqpOpenTelemetryInstrumenter create(boolean sender) {
        MessageOperation messageOperation = sender ? MessageOperation.PUBLISH : MessageOperation.RECEIVE;
        AmqpAttributesExtractor amqpAttributesExtractor = new AmqpAttributesExtractor();
        MessagingAttributesGetter<AmqpMessage<?>, Void> messagingAttributesGetter = amqpAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<AmqpMessage<?>, Void> builder = Instrumenter.builder(GlobalOpenTelemetry.get(),
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
