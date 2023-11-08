package ${package};

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;

import ${package}.api.BrokerClient;
import ${package}.api.ConsumedMessage;
import ${package}.tracing.${connectorPrefix}OpenTelemetryInstrumenter;
import ${package}.tracing.${connectorPrefix}Trace;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class ${connectorPrefix}IncomingChannel {

    private final String channel;
    private final BrokerClient client;
    private final Context context;
    private final ${connectorPrefix}AckHandler ackHandler;
    private final ${connectorPrefix}FailureHandler failureHandler;
    private final boolean tracingEnabled;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Flow.Publisher<? extends Message<?>> stream;

    public ${connectorPrefix}IncomingChannel(Vertx vertx, ${connectorPrefix}ConnectorIncomingConfiguration cfg, BrokerClient client) {
        // create and configure the client with ${connectorPrefix}ConnectorIncomingConfiguration
        this.channel = cfg.getChannel();
        this.client = client;
        this.context = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.ackHandler = ${connectorPrefix}AckHandler.create(this.client);
        this.failureHandler = ${connectorPrefix}FailureHandler.create(this.client);
        this.tracingEnabled = cfg.getTracingEnabled();
        Multi<? extends Message<?>> receiveMulti = Multi.createBy().repeating()
                .uni(() -> Uni.createFrom().completionStage(this.client.poll()))
                .until(__ -> closed.get())
                .emitOn(context::runOnContext)
                .map(consumed -> new ${connectorPrefix}Message<>(consumed, ackHandler, failureHandler));

        Instrumenter<${connectorPrefix}Trace, Void> instrumenter = ${connectorPrefix}OpenTelemetryInstrumenter.createInstrumenter(true);
        if (tracingEnabled) {
            receiveMulti = receiveMulti.map(message -> {
                ConsumedMessage<?> consumedMessage = message.getMetadata(${connectorPrefix}IncomingMetadata.class).get().getCustomMessage();
                return TracingUtils.traceIncoming(instrumenter, message, new ${connectorPrefix}Trace.Builder()
                        .withClientId(consumedMessage.clientId())
                        .withTopic(consumedMessage.topic())
                        .withProperties(consumedMessage.properties())
                        .build());
            });
        }
        this.stream = receiveMulti;
    }

    public String getChannel() {
        return channel;
    }

    public Flow.Publisher<? extends Message<?>> getStream() {
        return this.stream;
    }

    public void close() {
        closed.compareAndSet(false, true);
        client.close();
    }

    void isReady(HealthReport.HealthReportBuilder healthReportBuilder) {
        // TODO implement
    }

}
