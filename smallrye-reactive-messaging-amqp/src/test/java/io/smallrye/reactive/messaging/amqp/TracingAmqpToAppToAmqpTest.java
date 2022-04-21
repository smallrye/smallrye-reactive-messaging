package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessage;

public class TracingAmqpToAppToAmqpTest extends AmqpBrokerTestBase {

    private InMemorySpanExporter testExporter;
    private SpanProcessor spanProcessor;

    private WeldContainer container;
    private final Weld weld = new Weld();

    @BeforeEach
    public void setup() {
        super.setup();
        GlobalOpenTelemetry.resetForTest();

        testExporter = InMemorySpanExporter.create();
        spanProcessor = SimpleSpanProcessor.create(testExporter);

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .setSampler(Sampler.alwaysOn())
                .build();

        OpenTelemetrySdk.builder()
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());

        if (testExporter != null) {
            testExporter.shutdown();
        }
        if (spanProcessor != null) {
            spanProcessor.shutdown();
        }
    }

    @AfterAll
    static void shutdown() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    public void testFromAmqpToAppToAmqp() {
        List<Integer> payloads = new CopyOnWriteArrayList<>();
        List<Context> receivedContexts = new CopyOnWriteArrayList<>();
        usage.consumeIntegersWithTracing("result-topic",
                payloads::add,
                receivedContexts::add);

        weld.addBeanClass(MyAppProcessingData.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.result-topic.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.result-topic.durable", false)
                .put("mp.messaging.outgoing.result-topic.host", host)
                .put("mp.messaging.outgoing.result-topic.port", port)

                .put("mp.messaging.incoming.parent-topic.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.parent-topic.host", host)
                .put("mp.messaging.incoming.parent-topic.port", port)

                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        container = weld.initialize();
        MyAppProcessingData bean = container.getBeanManager().createInstance().select(MyAppProcessingData.class).get();

        await().until(() -> isAmqpConnectorReady(container));

        AtomicInteger count = new AtomicInteger();
        List<SpanContext> producedSpanContexts = new CopyOnWriteArrayList<>();
        usage.produce("parent-topic", 10, () -> AmqpMessage.create()
                .durable(false)
                .ttl(10000)
                .withIntegerAsBody(count.getAndIncrement())
                .applicationProperties(createTracingSpan(producedSpanContexts, "parent-topic"))
                .build());

        await().atMost(Duration.ofMinutes(5)).until(() -> payloads.size() >= 10);
        assertThat(payloads).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<String> producedTraceIds = producedSpanContexts.stream()
                .map(SpanContext::getTraceId)
                .collect(Collectors.toList());
        assertThat(producedTraceIds).hasSize(10);

        assertThat(receivedContexts).hasSize(10);
        assertThat(receivedContexts).doesNotContainNull().doesNotHaveDuplicates();

        List<String> receivedSpanIds = receivedContexts.stream()
                .map(context -> Span.fromContextOrNull(context).getSpanContext().getSpanId())
                .collect(Collectors.toList());
        assertThat(receivedSpanIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);

        List<String> receivedTraceIds = receivedContexts.stream()
                .map(context -> Span.fromContextOrNull(context).getSpanContext().getTraceId())
                .collect(Collectors.toList());
        assertThat(receivedTraceIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);
        assertThat(receivedTraceIds).containsExactlyInAnyOrderElementsOf(producedTraceIds);

        List<String> receivedParentSpanIds = new ArrayList<>();

        assertThat(bean.tracing()).hasSizeGreaterThanOrEqualTo(10);
        assertThat(bean.tracing()).doesNotContainNull().doesNotHaveDuplicates();
        List<String> spanIds = new ArrayList<>();

        for (TracingMetadata tracing : bean.tracing()) {
            Span span = Span.fromContext(tracing.getCurrentContext());
            spanIds.add(span.getSpanContext().getSpanId());
            assertThat(Span.fromContextOrNull(tracing.getPreviousContext())).isNotNull();
        }

        await().atMost(Duration.ofMinutes(2)).until(() -> testExporter.getFinishedSpanItems().size() >= 10);

        List<String> outgoingParentIds = new ArrayList<>();
        List<String> incomingParentIds = new ArrayList<>();

        for (SpanData data : testExporter.getFinishedSpanItems()) {
            if (data.getKind().equals(SpanKind.CONSUMER)) {
                incomingParentIds.add(data.getParentSpanId());
                // Need to skip the spans created during @Incoming processing
                continue;
            }
            assertThat(data.getSpanId()).isIn(receivedSpanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getTraceId()).isIn(producedTraceIds);
            assertThat(data.getKind()).isEqualByComparingTo(SpanKind.PRODUCER);
            outgoingParentIds.add(data.getParentSpanId());
        }

        // Assert span created on AMQP record is the parent of consumer span we create
        assertThat(producedSpanContexts.stream()
                .map(SpanContext::getSpanId)).containsExactlyElementsOf(incomingParentIds);

        // Assert consumer span is the parent of the producer span we received in AMQP
        assertThat(spanIds.stream())
                .containsExactlyElementsOf(outgoingParentIds);
    }

    private JsonObject createTracingSpan(List<SpanContext> spanContexts, String topic) {
        Properties properties = new Properties();
        final Span span = AmqpConnector.TRACER.spanBuilder(topic).setSpanKind(SpanKind.PRODUCER).startSpan();
        final Context context = span.storeInContext(Context.current());
        GlobalOpenTelemetry.getPropagators()
                .getTextMapPropagator()
                .inject(context, properties, (headers, key, value) -> {
                    if (headers != null) {
                        headers.put(key, value.getBytes(StandardCharsets.UTF_8));
                    }
                });
        spanContexts.add(span.getSpanContext());
        return JsonObject.mapFrom(properties);
    }

    @ApplicationScoped
    public static class MyAppProcessingData {
        private final List<TracingMetadata> tracingMetadata = new ArrayList<>();

        @Incoming("parent-topic")
        @Outgoing("result-topic")
        public Message<Integer> processMessage(Message<Integer> input) {
            tracingMetadata.add(input.getMetadata(TracingMetadata.class).orElse(TracingMetadata.empty()));
            return input.withPayload(input.getPayload() + 1);
        }

        public List<TracingMetadata> tracing() {
            return tracingMetadata;
        }
    }

}
