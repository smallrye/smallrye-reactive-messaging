package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
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
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class TracingAppToAmqpTest extends AmqpBrokerTestBase {

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

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFromAppToAmqp() {
        List<Integer> payloads = new CopyOnWriteArrayList<>();
        List<Context> contexts = new CopyOnWriteArrayList<>();
        usage.consumeIntegersWithTracing("amqp",
                payloads::add,
                contexts::add);

        weld.addBeanClass(MyAppGeneratingData.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.amqp.durable", false)
                .put("mp.messaging.outgoing.amqp.host", host)
                .put("mp.messaging.outgoing.amqp.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        container = weld.initialize();

        await().until(() -> isAmqpConnectorReady(container));

        await().until(() -> payloads.size() >= 10);
        assertThat(payloads).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(contexts).hasSize(10);
        assertThat(contexts).doesNotContainNull().doesNotHaveDuplicates();

        List<String> spanIds = contexts.stream()
                .map(context -> Span.fromContextOrNull(context).getSpanContext().getSpanId())
                .collect(Collectors.toList());
        assertThat(spanIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);

        List<String> traceIds = contexts.stream()
                .map(context -> Span.fromContextOrNull(context).getSpanContext().getTraceId())
                .collect(Collectors.toList());
        assertThat(traceIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);

        for (SpanData data : testExporter.getFinishedSpanItems()) {
            assertThat(data.getSpanId()).isIn(spanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getTraceId()).isIn(traceIds);
            assertThat(data.getKind()).isEqualByComparingTo(SpanKind.PRODUCER);
        }
    }

    @ApplicationScoped
    public static class MyAppGeneratingData {

        @Outgoing("amqp")
        public Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }
    }

}
