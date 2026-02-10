package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.google.cloud.pubsub.v1.Publisher;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
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
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PubSubTracingTest extends PubSubTestBase {

    private WeldContainer container;
    private SdkTracerProvider tracerProvider;
    private InMemorySpanExporter spanExporter;
    private Weld weld;
    private String topic;
    private String subscription;

    @BeforeEach
    public void setup(TestInfo testInfo) {
        topic = testInfo.getTestMethod().map(Method::getName).orElse("") + "_" + UUID.randomUUID();
        subscription = topic + "-subscription";
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

        initConfiguration(topic);
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            PubSubManager manager = container.select(PubSubManager.class).get();
            deleteTopicIfExists(manager, topic);
            container.shutdown();
        }
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        clear();
    }

    @AfterAll
    static void shutdown() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    public void testFromAppToPubSub() {
        weld = baseWeld();
        MapBasedConfig config = createSinkConfig("data", topic, PUBSUB_CONTAINER.getFirstMappedPort())
                .with("mp.messaging.outgoing.data.otel-enabled", true);

        addConfig(config);
        weld.addBeanClass(ProducerApp.class);
        container = weld.initialize();

        ProducerApp app = container.select(ProducerApp.class).get();

        await().until(() -> app.count() >= 10);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        await().untilAsserted(() -> assertThat(spans).hasSizeGreaterThanOrEqualTo(20));

        // Filter by instrumentation scope to separate SmallRye spans from GCP client spans
        List<SpanData> smallryeSpans = spans.stream()
                .filter(s -> "io.smallrye.reactive.messaging".equals(s.getInstrumentationScopeInfo().getName()))
                .toList();

        List<SpanData> gcpClientSpans = spans.stream()
                .filter(s -> "com.google.cloud.pubsub.v1".equals(s.getInstrumentationScopeInfo().getName()))
                .toList();

        // Verify SmallRye messaging-level PRODUCER spans (trace propagation)
        List<SpanData> smallryeProducerSpans = smallryeSpans.stream()
                .filter(s -> s.getKind() == SpanKind.PRODUCER)
                .toList();
        assertThat(smallryeProducerSpans).hasSize(10);

        // Verify GCP client-level PRODUCER spans
        List<SpanData> gcpProducerSpans = gcpClientSpans.stream()
                .filter(s -> s.getKind() == SpanKind.PRODUCER)
                .toList();
        assertThat(gcpProducerSpans).hasSize(10);

        // Verify SmallRye messaging-level spans attributes
        assertThat(smallryeProducerSpans).allSatisfy(s -> {
            assertThat(s.getAttributes().get(MESSAGING_SYSTEM)).isEqualTo("gcp_pubsub");
            assertThat(s.getAttributes().get(MESSAGING_OPERATION)).isEqualTo("publish");
            assertThat(s.getAttributes().get(MESSAGING_DESTINATION_NAME)).isEqualTo(topic);
        });

        // Verify GCP client-level spans attributes
        assertThat(gcpProducerSpans).allSatisfy(s -> {
            assertThat(s.getAttributes().get(MESSAGING_SYSTEM)).isEqualTo("gcp_pubsub");
            assertThat(s.getAttributes().get(MESSAGING_OPERATION)).isEqualTo("create");
            assertThat(s.getAttributes().get(MESSAGING_DESTINATION_NAME)).isEqualTo(topic);
        });
    }

    @Test
    public void testFromPubSubToApp() {
        weld = baseWeld();
        MapBasedConfig config = createSourceConfig("data", topic, subscription, PUBSUB_CONTAINER.getFirstMappedPort())
                .with("mp.messaging.incoming.data.otel-enabled", true);

        addConfig(config);
        weld.addBeanClass(ConsumerApp.class);
        container = weld.initialize();

        ConsumerApp app = container.select(ConsumerApp.class).get();

        // Wait until the subscription is ready
        PubSubManager manager = container.select(PubSubManager.class).get();
        createTopicIfNotExists(manager, topic);
        waitUntilSubscription(manager, topic, subscription);

        // Publish some test messages
        PubSubConfig pubSubConfig = new PubSubConfig(PROJECT_ID, topic, null, subscription,
                true, "localhost", PUBSUB_CONTAINER.getFirstMappedPort(), false);
        Publisher publisher = manager.publisher(pubSubConfig);

        for (int i = 0; i < 5; i++) {
            com.google.pubsub.v1.PubsubMessage message = com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(com.google.protobuf.ByteString.copyFromUtf8("test-" + i))
                    .build();
            publisher.publish(message);
        }

        await().until(() -> app.received().size() >= 5);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);

        await().untilAsserted(() -> assertThat(spanExporter.getFinishedSpanItems()).hasSizeGreaterThanOrEqualTo(10));

        List<SpanData> spans = spanExporter.getFinishedSpanItems();

        // Filter for SmallRye messaging-level CONSUMER spans (trace propagation spans)
        List<SpanData> smallryeConsumerSpans = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .filter(s -> "io.smallrye.reactive.messaging".equals(s.getInstrumentationScopeInfo().getName()))
                .toList();
        assertThat(smallryeConsumerSpans).hasSizeGreaterThanOrEqualTo(5);

        // Verify SmallRye messaging-level consumer span attributes
        // Note: SmallRye spans use the topic name, not subscription name
        assertThat(smallryeConsumerSpans).allSatisfy(span -> {
            assertThat(span.getAttributes().get(MESSAGING_SYSTEM)).isEqualTo("gcp_pubsub");
            assertThat(span.getAttributes().get(MESSAGING_DESTINATION_NAME)).isEqualTo(topic);
            assertThat(span.getAttributes().get(MESSAGING_OPERATION)).isEqualTo("receive");
        });

        // Also verify GCP client-level CONSUMER spans exist
        List<SpanData> gcpConsumerSpans = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .filter(s -> "com.google.cloud.pubsub.v1".equals(s.getInstrumentationScopeInfo().getName()))
                .toList();
        assertThat(gcpConsumerSpans).hasSizeGreaterThanOrEqualTo(5);

        // Verify GCP client-level spans use subscription name
        assertThat(gcpConsumerSpans).allSatisfy(span -> {
            assertThat(span.getAttributes().get(MESSAGING_SYSTEM)).isEqualTo("gcp_pubsub");
            assertThat(span.getAttributes().get(MESSAGING_DESTINATION_NAME)).isEqualTo(subscription);
        });
    }

    @Test
    public void testFromPubSubToAppToPubSub() {
        weld = baseWeld();

        MapBasedConfig config = createSourceConfig(topic, subscription, PUBSUB_CONTAINER.getFirstMappedPort())
                .with("mp.messaging.incoming.source.otel-enabled", true)
                .with("mp.messaging.outgoing.sink.connector", PubSubConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.topic", topic + "-out")
                .with("mp.messaging.outgoing.sink.otel-enabled", true);

        addConfig(config);
        weld.addBeanClass(ProcessorApp.class);
        container = weld.initialize();

        ProcessorApp app = container.select(ProcessorApp.class).get();

        // Publish some test messages
        PubSubManager manager = container.select(PubSubManager.class).get();
        createTopicIfNotExists(manager, topic);
        waitUntilSubscription(manager, topic, subscription);

        PubSubConfig pubSubConfig = new PubSubConfig(PROJECT_ID, topic, null, null,
                true, "localhost", PUBSUB_CONTAINER.getFirstMappedPort(), false);
        Publisher publisher = manager.publisher(pubSubConfig);

        for (int i = 0; i < 5; i++) {
            com.google.pubsub.v1.PubsubMessage message = com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(com.google.protobuf.ByteString.copyFromUtf8("test-" + i))
                    .build();
            publisher.publish(message);
        }

        await().until(() -> app.processed().size() >= 5);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);

        await().untilAsserted(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            // Verify we have spans created
            assertThat(spans).hasSizeGreaterThanOrEqualTo(10);

            // GCP Pub/Sub creates CLIENT spans for publish operations and CONSUMER spans for receive
            long clientSpans = spans.stream().filter(s -> s.getKind() == SpanKind.CLIENT).count();
            long producerSpans = spans.stream().filter(s -> s.getKind() == SpanKind.PRODUCER).count();
            long consumerSpans = spans.stream().filter(s -> s.getKind() == SpanKind.CONSUMER).count();
            assertThat(clientSpans).isGreaterThanOrEqualTo(5);
            assertThat(producerSpans).isGreaterThanOrEqualTo(5);
            assertThat(consumerSpans).isGreaterThanOrEqualTo(5);
        });

    }

    @ApplicationScoped
    public static class ProducerApp {
        private int count = 0;

        @Outgoing("data")
        public Multi<String> produce() {
            return Multi.createFrom().range(0, 10)
                    .onItem().invoke(() -> count++)
                    .onItem().transform(i -> "message-" + i);
        }

        public int count() {
            return count;
        }
    }

    @ApplicationScoped
    public static class ConsumerApp {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<String> message) {
            received.add(message.getPayload());
            return message.ack();
        }

        public List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ProcessorApp {
        private final List<String> processed = new CopyOnWriteArrayList<>();

        @Incoming("source")
        @Outgoing("sink")
        public Message<String> process(Message<String> message) {
            processed.add(message.getPayload());
            return message.withPayload("processed-" + message.getPayload());
        }

        public List<String> processed() {
            return processed;
        }
    }
}
