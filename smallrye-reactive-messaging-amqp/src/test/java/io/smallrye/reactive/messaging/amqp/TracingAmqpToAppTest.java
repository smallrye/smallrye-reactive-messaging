package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.amqp.AmqpMessage;

@Disabled("See https://github.com/smallrye/smallrye-reactive-messaging/issues/1268")
public class TracingAmqpToAppTest extends AmqpBrokerTestBase {
    private SdkTracerProvider tracerProvider;
    private InMemorySpanExporter spanExporter;

    private WeldContainer container;
    private final Weld weld = new Weld();

    @BeforeEach
    public void setup() {
        super.setup();
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

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @AfterAll
    static void shutdown() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    public void testFromAmqpToAppWithParentSpan() {
        new MapBasedConfig()
                .with("mp.messaging.incoming.stuff.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.stuff.host", host)
                .with("mp.messaging.incoming.stuff.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyAppReceivingData.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));

        MyAppReceivingData bean = container.getBeanManager().createInstance().select(MyAppReceivingData.class).get();

        AtomicInteger count = new AtomicInteger();
        usage.produce("stuff", 10, () -> AmqpMessage.create()
                .durable(false)
                .ttl(10000)
                .withIntegerAsBody(count.getAndIncrement())
                .build());

        await().until(() -> bean.results().size() >= 10);
        assertThat(bean.results()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());
        });
    }

    @ApplicationScoped
    public static class MyAppReceivingData {
        private final List<Integer> results = new CopyOnWriteArrayList<>();

        @Incoming("stuff")
        public CompletionStage<Void> consume(Message<Integer> input) {
            results.add(input.getPayload());
            return input.ack();
        }

        public List<Integer> results() {
            return results;
        }
    }
}
