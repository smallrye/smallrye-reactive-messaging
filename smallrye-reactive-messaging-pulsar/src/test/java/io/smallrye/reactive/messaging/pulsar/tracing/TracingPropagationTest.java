package io.smallrye.reactive.messaging.pulsar.tracing;

import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_SYSTEM;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanId;
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
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

public class TracingPropagationTest extends WeldTestBase {
    private SdkTracerProvider tracerProvider;
    private InMemorySpanExporter spanExporter;

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
    public void testFromAppToPulsar() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<Integer>> messages = new ArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(topic + "-consumer")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe(), 10, messages::add);

        runApplication(getConfigForMyAppGeneratingData(), MyAppGeneratingData.class);

        await().until(() -> messages.size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(messages).allSatisfy(record -> {
            assertThat(record.getValue()).isNotNull();
            values.add(record.getValue());
        });
        assertThat(values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());

            assertEquals(10, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

            SpanData span = spans.get(0);
            assertEquals(SpanKind.PRODUCER, span.getKind());
            assertEquals("pulsar", span.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals(topic + " publish", span.getName());
        });
    }

    @Test
    @Disabled
    public void testFromAppToPulsarWithStructuredCloudEvents() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<String>> messages = new ArrayList<>();
        receive(client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(topic + "-consumer")
                .subscribe(), 10, messages::add);
        runApplication(getConfigForMyAppGeneratingDataWithStructuredCloudEvent("structured"),
                MyAppGeneratingCloudEventData.class);

        await().until(() -> messages.size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(messages).allSatisfy(message -> {
            assertThat(message.getValue()).isNotNull();
            JsonObject ce = (JsonObject) Json.decodeValue(message.getValue());
            assertThat(ce.getString("source")).isEqualTo("test://test");
            assertThat(ce.getString("type")).isEqualTo("type");
            values.add(Integer.parseInt(ce.getString("data")));
        });
        assertThat(values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());

            assertEquals(10, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

            SpanData span = spans.get(0);
            assertEquals(SpanKind.PRODUCER, span.getKind());
            assertEquals("kafka", span.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals(topic + " publish", span.getName());
        });
    }

    @Test
    @Disabled
    public void testFromAppToPulsarWithBinaryCloudEvents() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<Integer>> messages = new ArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(topic + "-consumer")
                .subscribe(), 10, messages::add);
        runApplication(getConfigForMyAppGeneratingDataWithStructuredCloudEvent("binary"),
                MyAppGeneratingCloudEventData.class);

        List<Integer> values = new ArrayList<>();
        assertThat(messages).allSatisfy(message -> {
            assertThat(message.getValue()).isNotNull();
            assertThat(message.getProperties().get("ce_source"))
                    .isEqualTo("test://test".getBytes(StandardCharsets.UTF_8));
            assertThat(message.getProperties().get("ce_type")).isEqualTo("type".getBytes(StandardCharsets.UTF_8));
            values.add(message.getValue());
        });
        assertThat(values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());

            assertEquals(10, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

            SpanData span = spans.get(0);
            assertEquals(SpanKind.PRODUCER, span.getKind());
            assertEquals("kafka", span.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals(topic + " publish", span.getName());
        });
    }

    @Test
    public void testFromPulsarToAppToPulsar() throws PulsarClientException {
        String resultTopic = topic + "-result";
        String parentTopic = topic + "-parent";
        List<org.apache.pulsar.client.api.Message> messages = new ArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .topic(resultTopic)
                .subscriptionName(topic + "-consumer")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe(), 10, messages::add);

        runApplication(getConfigForMyAppProcessingData(resultTopic, parentTopic), MyAppProcessingData.class);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(parentTopic)
                .create(), 10, (i, p) -> p.newMessage().key("a-key").value(i));

        await().until(() -> messages.size() == 10);
        assertThat(messages)
                .extracting(org.apache.pulsar.client.api.Message::getValue)
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(messages)
                .extracting(org.apache.pulsar.client.api.Message::getProperties)
                .allSatisfy(m -> assertThat(m).containsKey("traceparent"));

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(20, spans.size());

            List<SpanData> parentSpans = spans.stream()
                    .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid()))
                    .collect(toList());
            assertEquals(10, parentSpans.size());

            for (SpanData parentSpan : parentSpans) {
                assertEquals(1,
                        spans.stream().filter(spanData -> spanData.getParentSpanId().equals(parentSpan.getSpanId())).count());
            }

            SpanData consumer = parentSpans.get(0);
            assertEquals(SpanKind.CONSUMER, consumer.getKind());
            assertEquals("persistent://public/default/" + parentTopic + " receive", consumer.getName());

            SpanData producer = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(consumer.getSpanId()))
                    .findFirst().get();
            assertEquals(SpanKind.PRODUCER, producer.getKind());
            assertEquals("pulsar", producer.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals(resultTopic + " publish", producer.getName());
        });
    }

    @Test
    public void testFromPulsarToAppWithParentSpan() throws PulsarClientException {
        String parentTopic = topic + "-parent";
        MyAppReceivingData bean = runApplication(getConfigForMyAppReceivingData(parentTopic),
                MyAppReceivingData.class);

        Map<String, String> headers = new HashMap<>();
        try (Scope ignored = Context.current().makeCurrent()) {
            Tracer tracer = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging");
            Span span = tracer.spanBuilder("producer").setSpanKind(SpanKind.PRODUCER).startSpan();
            Context current = Context.current().with(span);
            GlobalOpenTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(current, headers, (carrier, key, value) -> carrier.put(key, value));
            span.end();
        }

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(parentTopic)
                .create(), 10, (i, p) -> p.newMessage().key("a-key").value(i).properties(headers));

        await().until(() -> bean.results().size() >= 10);
        assertThat(bean.results()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            // 1 Parent, 10 Children
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(11, spans.size());

            // All should use the same Trace
            assertEquals(1, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

            List<SpanData> parentSpans = spans.stream()
                    .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid())).collect(toList());
            assertEquals(1, parentSpans.size());

            for (SpanData parentSpan : parentSpans) {
                assertEquals(10,
                        spans.stream().filter(spanData -> spanData.getParentSpanId().equals(parentSpan.getSpanId())).count());
            }

            SpanData producer = parentSpans.get(0);
            assertEquals(SpanKind.PRODUCER, producer.getKind());

            SpanData consumer = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(producer.getSpanId()))
                    .findFirst().get();
            assertEquals("pulsar", consumer.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("persistent://public/default/" + parentTopic + " receive", consumer.getName());
        });
    }

    @Test
    public void testFromPulsarToAppWithNoParent() throws PulsarClientException {
        MyAppReceivingData bean = runApplication(
                getConfigForMyAppReceivingData(topic), MyAppReceivingData.class);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(), 10, (i, p) -> p.newMessage().key("a-key").value(i));

        await().until(() -> bean.results().size() >= 10);
        assertThat(bean.results()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());

            for (SpanData span : spans) {
                assertEquals(SpanKind.CONSUMER, span.getKind());
                assertEquals(SpanId.getInvalid(), span.getParentSpanId());
            }
        });
    }

    private MapBasedConfig getConfigForMyAppGeneratingData() {
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.topic", topic)
                .with("mp.messaging.outgoing.pulsar.schema", "INT32");
    }

    private MapBasedConfig getConfigForMyAppGeneratingDataWithStructuredCloudEvent(String mode) {
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.topic", topic)
                .with("mp.messaging.outgoing.pulsar.schema", "STRING")
                .with("mp.messaging.outgoing.pulsar.cloud-events-mode", mode);
    }

    private MapBasedConfig getConfigForMyAppProcessingData(String resultTopic, String parentTopic) {
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.topic", resultTopic)
                .with("mp.messaging.outgoing.pulsar.schema", "INT32")
                .with("mp.messaging.incoming.source.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.source.topic", parentTopic)
                .with("mp.messaging.incoming.source.schema", "INT32")
                .with("mp.messaging.incoming.source.subscriptionInitialPosition", "Earliest");
    }

    private MapBasedConfig getConfigForMyAppReceivingData(String topic) {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.stuff.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.stuff.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.stuff.topic", topic)
                .with("mp.messaging.incoming.stuff.schema", "INT32")
                .with("mp.messaging.incoming.stuff.subscriptionInitialPosition", "Earliest");
    }

    @ApplicationScoped
    public static class MyAppGeneratingData {
        @Outgoing("pulsar")
        public Flow.Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }
    }

    @ApplicationScoped
    public static class MyAppGeneratingCloudEventData {
        @Outgoing("pulsar")
        public Flow.Publisher<Message<String>> source() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> Message.of(Integer.toString(i)))
                    .map(m -> m.addMetadata(OutgoingCloudEventMetadata.builder()
                            .withSource(URI.create("test://test"))
                            .withType("type")
                            .withId("some id")
                            .build()));
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingData {
        @Incoming("source")
        @Outgoing("pulsar")
        public Message<Integer> processMessage(Message<Integer> input) {
            return input.withPayload(input.getPayload() + 1);
        }
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
