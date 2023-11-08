package ${package}.tracing;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

/**
 * Encapsulates the OpenTelemetry instrumentation API so that those classes are only needed if
 * users explicitly enable tracing.
 */
public class ${connectorPrefix}OpenTelemetryInstrumenter {

    private final Instrumenter<${connectorPrefix}Trace, Void> instrumenter;

    private ${connectorPrefix}OpenTelemetryInstrumenter(Instrumenter<${connectorPrefix}Trace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static ${connectorPrefix}OpenTelemetryInstrumenter createIncoming() {
        return new ${connectorPrefix}OpenTelemetryInstrumenter(createInstrumenter(true));
    }

    public static ${connectorPrefix}OpenTelemetryInstrumenter createOutgoing() {
        return new ${connectorPrefix}OpenTelemetryInstrumenter(createInstrumenter(false));
    }

    // <create-instrumenter>
    public static Instrumenter<${connectorPrefix}Trace, Void> createInstrumenter(boolean incoming) {
        MessageOperation messageOperation = incoming ? MessageOperation.RECEIVE : MessageOperation.PUBLISH;

        ${connectorPrefix}AttributesExtractor myExtractor = new ${connectorPrefix}AttributesExtractor();
        MessagingAttributesGetter<${connectorPrefix}Trace, Void> attributesGetter = myExtractor.getMessagingAttributesGetter();
        var spanNameExtractor = MessagingSpanNameExtractor.create(attributesGetter, messageOperation);
        InstrumenterBuilder<${connectorPrefix}Trace, Void> builder = Instrumenter.builder(GlobalOpenTelemetry.get(),
                "io.smallrye.reactive.messaging", spanNameExtractor);
        var attributesExtractor = MessagingAttributesExtractor.create(attributesGetter, messageOperation);

        builder
                .addAttributesExtractor(attributesExtractor)
                .addAttributesExtractor(myExtractor);

        if (incoming) {
            return builder.buildConsumerInstrumenter(${connectorPrefix}TraceTextMapGetter.INSTANCE);
        } else {
            return builder.buildProducerInstrumenter(${connectorPrefix}TraceTextMapSetter.INSTANCE);
        }
    }
    // </create-instrumener>

    public Message<?> traceIncoming(Message<?> message, ${connectorPrefix}Trace myTrace, boolean makeCurrent) {
        return TracingUtils.traceIncoming(instrumenter, message, myTrace, makeCurrent);
    }

    public void traceOutgoing(Message<?> message, ${connectorPrefix}Trace myTrace) {
        TracingUtils.traceOutgoing(instrumenter, message, myTrace);
    }
}
