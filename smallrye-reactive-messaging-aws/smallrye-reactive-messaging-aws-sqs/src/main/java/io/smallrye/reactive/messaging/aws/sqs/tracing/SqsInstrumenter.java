package io.smallrye.reactive.messaging.aws.sqs.tracing;

import static io.smallrye.reactive.messaging.tracing.TracingUtils.INSTRUMENTATION_NAME;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;

public class SqsInstrumenter {

    public static final Instrumenter<SqsTrace, Void> SQS_OUTGOING_INSTRUMENTER;

    static {
        final InstrumenterBuilder<SqsTrace, Void> instrumenterBuilder = Instrumenter.builder(GlobalOpenTelemetry.get(),
                INSTRUMENTATION_NAME, MessagingSpanNameExtractor.create(
                        SqsMessagingAttributesGetter.INSTANCE, MessageOperation.PUBLISH));

        SQS_OUTGOING_INSTRUMENTER = instrumenterBuilder
                .addAttributesExtractor(MessagingAttributesExtractor.create(
                        SqsMessagingAttributesGetter.INSTANCE, MessageOperation.PUBLISH))
                .addAttributesExtractor(SqsAttributesExtractor.INSTANCE)
                .buildProducerInstrumenter(SqsTraceTextMapSetter.INSTANCE);
    }

}
