package connectors.tracing;

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
public class MyOpenTelemetryInstrumenter {

    private final Instrumenter<MyTrace, Void> instrumenter;

    private MyOpenTelemetryInstrumenter(Instrumenter<MyTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static MyOpenTelemetryInstrumenter createIncoming() {
        return new MyOpenTelemetryInstrumenter(createInstrumenter(true));
    }

    public static MyOpenTelemetryInstrumenter createOutgoing() {
        return new MyOpenTelemetryInstrumenter(createInstrumenter(false));
    }

    // <create-instrumenter>
    public static Instrumenter<MyTrace, Void> createInstrumenter(boolean incoming) {
        MessageOperation messageOperation = incoming ? MessageOperation.RECEIVE : MessageOperation.PUBLISH;

        MyAttributesExtractor myExtractor = new MyAttributesExtractor();
        MessagingAttributesGetter<MyTrace, Void> attributesGetter = myExtractor.getMessagingAttributesGetter();
        var spanNameExtractor = MessagingSpanNameExtractor.create(attributesGetter, messageOperation);
        InstrumenterBuilder<MyTrace, Void> builder = Instrumenter.builder(GlobalOpenTelemetry.get(),
                "io.smallrye.reactive.messaging", spanNameExtractor);
        var attributesExtractor = MessagingAttributesExtractor.create(attributesGetter, messageOperation);

        builder
                .addAttributesExtractor(attributesExtractor)
                .addAttributesExtractor(myExtractor);

        if (incoming) {
            return builder.buildConsumerInstrumenter(MyTraceTextMapGetter.INSTANCE);
        } else {
            return builder.buildProducerInstrumenter(MyTraceTextMapSetter.INSTANCE);
        }
    }
    // </create-instrumener>

    public Message<?> traceIncoming(Message<?> message, MyTrace myTrace, boolean makeCurrent) {
        return TracingUtils.traceIncoming(instrumenter, message, myTrace, makeCurrent);
    }

    public void traceOutgoing(Message<?> message, MyTrace myTrace) {
        TracingUtils.traceOutgoing(instrumenter, message, myTrace);
    }
}
