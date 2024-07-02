package io.smallrye.reactive.messaging.amqp;

import static io.opentelemetry.api.trace.SpanKind.CONSUMER;
import static io.opentelemetry.api.trace.SpanKind.PRODUCER;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_OPERATION;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_SYSTEM;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

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
import io.opentelemetry.api.trace.SpanId;
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
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessage;

public class TracingAmqpToAppToAmqpTest extends AmqpBrokerTestBase {
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
    public void testFromAmqpToAppToAmqp() {
        new MapBasedConfig()
                .with("mp.messaging.outgoing.result-topic.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.result-topic.durable", false)
                .with("mp.messaging.outgoing.result-topic.host", host)
                .with("mp.messaging.outgoing.result-topic.port", port)
                .with("mp.messaging.incoming.parent-topic.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.parent-topic.host", host)
                .with("mp.messaging.incoming.parent-topic.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyAppProcessingData.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        List<AmqpMessage> messages = new CopyOnWriteArrayList<>();
        usage.consume("result-topic", messages::add);

        AtomicInteger count = new AtomicInteger();
        usage.produce("parent-topic", 10, () -> AmqpMessage.create()
                .durable(false)
                .ttl(10000)
                .withIntegerAsBody(count.getAndIncrement())
                .build());

        await().until(() -> messages.size() >= 10);
        assertThat(messages)
                .extracting(AmqpMessage::bodyAsInteger)
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(messages)
                .extracting(m -> m.applicationProperties().getMap())
                .allSatisfy(m -> assertThat(m).containsKey("my-property").containsKey("traceparent"));

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(20, spans.size());

            List<SpanData> parentSpans = spans.stream()
                    .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid())).collect(toList());
            assertEquals(10, parentSpans.size());

            for (SpanData parentSpan : parentSpans) {
                assertEquals(1,
                        spans.stream().filter(spanData -> spanData.getParentSpanId().equals(parentSpan.getSpanId())).count());
            }

            SpanData consumer = parentSpans.get(0);
            assertEquals(CONSUMER, consumer.getKind());
            assertEquals("AMQP 1.0", consumer.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("parent-topic", consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("parent-topic receive", consumer.getName());
            assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));

            SpanData producer = spans.stream().filter(span -> span.getParentSpanId().equals(consumer.getSpanId())).findFirst()
                    .get();
            assertEquals(PRODUCER, producer.getKind());
            assertEquals("result-topic", producer.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("result-topic publish", producer.getName());
            assertEquals("publish", producer.getAttributes().get(MESSAGING_OPERATION));
        });
    }

    @ApplicationScoped
    public static class MyAppProcessingData {
        @Incoming("parent-topic")
        @Outgoing("result-topic")
        public Message<Integer> processMessage(Message<Integer> input) {
            int newPayload = input.getPayload() + 1;
            return input.withPayload(newPayload)
                    .addMetadata(OutgoingAmqpMetadata.builder()
                            .withApplicationProperties(JsonObject.of("my-property", newPayload)).build());
        }
    }
}
