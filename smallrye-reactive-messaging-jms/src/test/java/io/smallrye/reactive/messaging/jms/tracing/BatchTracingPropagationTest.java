package io.smallrye.reactive.messaging.jms.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.smallrye.reactive.messaging.jms.*;
import io.smallrye.reactive.messaging.jms.PayloadConsumerBean;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class BatchTracingPropagationTest extends JmsTestBase {
    private SdkTracerProvider tracerProvider;
    private InMemorySpanExporter spanExporter;

    private JMSContext jms;
    private ActiveMQJMSConnectionFactory factory;
    private Flow.Subscriber<org.eclipse.microprofile.reactive.messaging.Message<?>> subscriber;

    @BeforeEach
    public void init() {
        factory = new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
        jms = factory.createContext();
    }

    @AfterEach
    public void close() {
        jms.close();
        factory.close();
    }

    private JmsResourceHolder<JMSProducer> getResourceHolder() {
        return new JmsResourceHolder<>("jms", () -> jms);
    }

    @BeforeEach
    public void setup() {
        GlobalOpenTelemetry.resetForTest();

        spanExporter = InMemorySpanExporter.create();
        SpanProcessor spanProcessor = SimpleSpanProcessor.create(spanExporter);

        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .setSampler(Sampler.alwaysOn())
                .build();

        OpenTelemetrySdk.builder()
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
    }

    @AfterAll
    static void shutdown() {
        GlobalOpenTelemetry.resetForTest();
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFromAppToJms() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class, ProducerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();
        await().until(() -> bean.list().size() >= 10);

        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(20, spans.size());
            SpanData spanData = spans.get(19);
            assertEquals(SpanKind.CONSUMER, spanData.getKind());
        });
    }

    @Test
    public void testFromJmsToAppWithParentSpan() {
        String parentTopic = "queue-one-parent";
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class, ProducerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();

        Map<String, Object> headers = new HashMap<>();
        try (Scope ignored = Context.current().makeCurrent()) {
            Tracer tracer = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging");
            Span span = tracer.spanBuilder("producer").setSpanKind(SpanKind.PRODUCER).startSpan();
            Context current = Context.current().with(span);
            GlobalOpenTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(current, headers, (carrier, key, value) -> carrier.put(key, value.getBytes()));
            span.end();
        }

        await().until(() -> bean.list().size() >= 10);

        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(21, spans.size());
        });
    }

    @Test
    public void testFromJmsToAppWithNoParent() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class, ProducerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();

        await().until(() -> bean.list().size() >= 10);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(20, spans.size());
        });
    }
}
