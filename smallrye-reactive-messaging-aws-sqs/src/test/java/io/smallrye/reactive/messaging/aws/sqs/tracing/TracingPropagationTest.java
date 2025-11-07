package io.smallrye.reactive.messaging.aws.sqs.tracing;

import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
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
import io.smallrye.reactive.messaging.aws.sqs.SqsClientProvider;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnector;
import io.smallrye.reactive.messaging.aws.sqs.SqsIncomingMetadata;
import io.smallrye.reactive.messaging.aws.sqs.SqsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

public class TracingPropagationTest extends SqsTestBase {
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
    public void testFromAppToSqs() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class, ProducerApp.class);
        String queueUrl = createQueue(queue);
        MyAppReceivingData bean = runApplication(new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue)
                .with("mp.messaging.outgoing.sink.tracing-enabled", true)
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.tracing-enabled", true),
                MyAppReceivingData.class);

        await().until(() -> bean.results().size() >= 10);
        assertThat(bean.results())
                .extracting(m -> m.getMetadata(SqsIncomingMetadata.class).get())
                .allSatisfy(m -> assertThat(m.getMessage().messageAttributes().get("traceparent")).isNotNull())
                .extracting(m -> Integer.parseInt(m.getMessage().body()))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(20, spans.size());

        assertEquals(10, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

        SpanData span = spans.get(0);
        assertEquals(SpanKind.PRODUCER, span.getKind());
        assertEquals(3, span.getAttributes().size());
        assertEquals("sqs", span.getAttributes().get(MESSAGING_SYSTEM));
        assertEquals("publish", span.getAttributes().get(MESSAGING_OPERATION));
        assertEquals(queueUrl, span.getAttributes().get(MESSAGING_DESTINATION_NAME));
        assertEquals(queueUrl + " publish", span.getName());
    }

    @Test
    public void testFromSqsToAppToSqs() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        String inQueueUrl = createQueue(queue);
        String resultQueue = queue + "-result";
        String resultQueueUrl = createQueue(resultQueue);
        MyAppProcessingData myAppProcessingData = runApplication(new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.queue", queue)
                .with("mp.messaging.incoming.source.tracing-enabled", true)
                .with("mp.messaging.outgoing.sqs.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sqs.queue", resultQueue)
                .with("mp.messaging.outgoing.sqs.tracing-enabled", true), MyAppProcessingData.class);

        sendMessage(inQueueUrl, 10, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                        .dataType("String").stringValue(Integer.class.getName()).build())));

        var messages = receiveMessages(resultQueueUrl, 10, Duration.ofSeconds(10));
        assertThat(messages)
                .extracting(m -> Integer.valueOf(m.body()))
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(20, spans.size());

        List<SpanData> parentSpans = spans.stream()
                .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid()))
                .collect(toList());
        assertEquals(10, parentSpans.size());

        SpanData consumer = parentSpans.get(0);
        assertEquals(SpanKind.CONSUMER, consumer.getKind());
        assertEquals(4, consumer.getAttributes().size());
        assertEquals("sqs", consumer.getAttributes().get(MESSAGING_SYSTEM));
        assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));
        assertEquals(inQueueUrl, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
        assertEquals(inQueueUrl + " receive", consumer.getName());

        for (SpanData span : spans) {
            System.out.println(span.getKind() + " " + span.getSpanId() + " -> " + span.getParentSpanId());
        }
        SpanData producerSpan = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(consumer.getSpanId()))
                .findFirst().get();
        assertEquals(SpanKind.PRODUCER, producerSpan.getKind());
        assertEquals(3, producerSpan.getAttributes().size());
        assertEquals("sqs", producerSpan.getAttributes().get(MESSAGING_SYSTEM));
        assertEquals("publish", producerSpan.getAttributes().get(MESSAGING_OPERATION));
        assertEquals(resultQueueUrl, producerSpan.getAttributes().get(MESSAGING_DESTINATION_NAME));
        assertEquals(resultQueueUrl + " publish", producerSpan.getName());
    }

    @Test
    public void testFromSqsToAppWithParentSpan() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        String queueUrl = createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue);
        MyAppReceivingPayload bean = runApplication(config, MyAppReceivingPayload.class);

        Map<String, String> properties = new HashMap<>();
        try (Scope ignored = Context.current().makeCurrent()) {
            Tracer tracer = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging");
            Span span = tracer.spanBuilder("producer").setSpanKind(SpanKind.PRODUCER).startSpan();
            Context current = Context.current().with(span);
            GlobalOpenTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(current, properties, (carrier, key, value) -> carrier.put(key, value));
            span.end();
        }

        sendMessage(queueUrl, 10, (i, r) -> {
            HashMap<String, MessageAttributeValue> attributes = new HashMap<>();
            attributes.put("_classname", MessageAttributeValue.builder()
                    .stringValue("java.lang.Integer").dataType("String").build());
            properties.forEach((k, v) -> attributes.put(k, MessageAttributeValue.builder()
                    .stringValue(v).dataType("String").build()));
            r.messageAttributes(attributes)
                    .messageBody(String.valueOf(i));
        });

        await().until(() -> bean.list().size() >= 10);

        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);
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

        SpanData producerSpan = parentSpans.get(0);
        assertEquals(SpanKind.PRODUCER, producerSpan.getKind());

        SpanData consumer = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(producerSpan.getSpanId()))
                .findFirst().get();
        assertEquals(4, consumer.getAttributes().size());
        assertEquals("sqs", consumer.getAttributes().get(MESSAGING_SYSTEM));
        assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));
        assertEquals(queueUrl, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
        assertEquals(queueUrl + " receive", consumer.getName());

    }

    @Test
    public void testFromSqsToAppWithNoParent() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        String queueUrl = createQueue(queue);
        MyAppReceivingPayload bean = runApplication(new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.tracing-enabled", true), MyAppReceivingPayload.class);

        sendMessage(queueUrl, 10, (i, r) -> {
            Map<String, MessageAttributeValue> attributes = new HashMap<>();
            attributes.put("_classname", MessageAttributeValue.builder()
                    .stringValue("java.lang.Integer").dataType("String").build());
            r.messageAttributes(attributes)
                    .messageBody(Integer.toString(i));
        });

        await().until(() -> bean.list().size() >= 10);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(10).allSatisfy(span -> {
            assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(span.getSpanId()).isNotEqualTo(SpanId.getInvalid());
            assertThat(span.getParentSpanId()).isEqualTo(SpanId.getInvalid());
        });
    }

    @ApplicationScoped
    public static class MyAppProcessingData {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        @Incoming("source")
        @Outgoing("sqs")
        public Message<Integer> processMessage(Message<Integer> input) {
            list.add(input.getPayload());
            return input.withPayload(input.getPayload() + 1);
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MyAppReceivingData {
        private final List<Message<String>> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<String> input) {
            results.add(input);
            return input.ack();
        }

        public List<Message<String>> results() {
            return results;
        }
    }

    @ApplicationScoped
    public static class MyAppReceivingPayload {
        private final List<Integer> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Integer input) {
            results.add(input);
        }

        public List<Integer> list() {
            return results;
        }
    }

    @ApplicationScoped
    public static class ProducerApp {

        @Outgoing("sink")
        Multi<Integer> produce() {
            return Multi.createFrom().range(0, 10);
        }

    }

}
