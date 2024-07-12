package io.smallrye.reactive.messaging.kafka.tracing;

import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_CLIENT_ID;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_KAFKA_MESSAGE_OFFSET;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_OPERATION;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_SYSTEM;
import static io.smallrye.reactive.messaging.kafka.companion.RecordQualifiers.until;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

public class TracingPropagationTest extends KafkaCompanionTestBase {
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
    public void testFromAppToKafka() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic,
                m -> m.plug(until(10L, Duration.ofMinutes(1), null)));
        runApplication(getKafkaSinkConfigForMyAppGeneratingData(), MyAppGeneratingData.class);

        await().until(() -> consumed.getRecords().size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(consumed.getRecords()).allSatisfy(record -> {
            assertThat(record.value()).isNotNull();
            values.add(record.value());
        });
        assertThat(values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());

            assertEquals(10, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

            SpanData span = spans.get(0);
            assertEquals(SpanKind.PRODUCER, span.getKind());
            assertEquals(5, span.getAttributes().size());
            assertEquals("kafka", span.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("publish", span.getAttributes().get(MESSAGING_OPERATION));
            assertEquals(topic, span.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("kafka-producer-kafka", span.getAttributes().get(MESSAGING_CLIENT_ID));
            assertEquals(0, span.getAttributes().get(MESSAGING_KAFKA_MESSAGE_OFFSET));

            assertEquals(topic + " publish", span.getName());
        });
    }

    @Test
    public void testFromAppToKafkaWithStructuredCloudEvents() {
        ConsumerTask<String, String> consumed = companion.consumeStrings().fromTopics(topic,
                m -> m.plug(until(10L, Duration.ofMinutes(1), null)));
        runApplication(getKafkaSinkConfigForMyAppGeneratingDataWithStructuredCloudEvent("structured"),
                MyAppGeneratingCloudEventData.class);

        await().until(() -> consumed.getRecords().size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(consumed.getRecords()).allSatisfy(record -> {
            assertThat(record.value()).isNotNull();
            JsonObject ce = (JsonObject) Json.decodeValue(record.value());
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
            assertEquals(5, span.getAttributes().size());
            assertEquals("kafka", span.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("publish", span.getAttributes().get(MESSAGING_OPERATION));
            assertEquals(topic, span.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("publish", span.getAttributes().get(MESSAGING_OPERATION));
            assertEquals("kafka-producer-kafka", span.getAttributes().get(MESSAGING_CLIENT_ID));
            assertEquals(0, span.getAttributes().get(MESSAGING_KAFKA_MESSAGE_OFFSET));
            assertEquals(topic + " publish", span.getName());
        });
    }

    @Test
    public void testFromAppToKafkaWithBinaryCloudEvents() {
        ConsumerTask<String, String> consumed = companion.consumeStrings().fromTopics(topic,
                m -> m.plug(until(10L, Duration.ofMinutes(1), null)));
        runApplication(getKafkaSinkConfigForMyAppGeneratingDataWithStructuredCloudEvent("binary"),
                MyAppGeneratingCloudEventData.class);

        await().until(() -> consumed.getRecords().size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(consumed.getRecords()).allSatisfy(record -> {
            assertThat(record.value()).isNotNull();
            assertThat(record.headers().lastHeader("ce_source").value())
                    .isEqualTo("test://test".getBytes(StandardCharsets.UTF_8));
            assertThat(record.headers().lastHeader("ce_type").value()).isEqualTo("type".getBytes(StandardCharsets.UTF_8));
            values.add(Integer.parseInt(record.value()));
        });
        assertThat(values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());

            assertEquals(10, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

            SpanData span = spans.get(0);
            assertEquals(SpanKind.PRODUCER, span.getKind());
            assertEquals(5, span.getAttributes().size());
            assertEquals("kafka", span.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("publish", span.getAttributes().get(MESSAGING_OPERATION));
            assertEquals(topic, span.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("publish", span.getAttributes().get(MESSAGING_OPERATION));
            assertEquals("kafka-producer-kafka", span.getAttributes().get(MESSAGING_CLIENT_ID));
            assertEquals(0, span.getAttributes().get(MESSAGING_KAFKA_MESSAGE_OFFSET));
            assertEquals(topic + " publish", span.getName());
        });
    }

    @Test
    public void testFromKafkaToAppToKafka() {
        String resultTopic = topic + "-result";
        String parentTopic = topic + "-parent";
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(resultTopic,
                m -> m.plug(until(10L, Duration.ofMinutes(1), null)));
        runApplication(getKafkaSinkConfigForMyAppProcessingData(resultTopic, parentTopic), MyAppProcessingData.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(parentTopic, null, null, "a-key", i, new ArrayList<>()), 10);

        await().until(() -> consumed.getRecords().size() == 10);
        List<Integer> values = new ArrayList<>();
        assertThat(consumed.getRecords()).allSatisfy(record -> {
            assertThat(record.value()).isNotNull();
            values.add(record.value());
        });
        assertThat(values).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

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
            assertEquals(8, consumer.getAttributes().size());
            assertEquals("kafka", consumer.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));
            assertEquals(parentTopic, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("kafka-consumer-source", consumer.getAttributes().get(MESSAGING_CLIENT_ID));
            assertEquals(0, consumer.getAttributes().get(MESSAGING_KAFKA_MESSAGE_OFFSET));
            assertEquals(parentTopic + " receive", consumer.getName());

            SpanData producer = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(consumer.getSpanId()))
                    .findFirst().get();
            assertEquals(SpanKind.PRODUCER, producer.getKind());
            assertEquals(5, producer.getAttributes().size());
            assertEquals("kafka", producer.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("publish", producer.getAttributes().get(MESSAGING_OPERATION));
            assertEquals(resultTopic, producer.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("publish", producer.getAttributes().get(MESSAGING_OPERATION));
            assertEquals("kafka-producer-kafka", producer.getAttributes().get(MESSAGING_CLIENT_ID));
            assertEquals(0, producer.getAttributes().get(MESSAGING_KAFKA_MESSAGE_OFFSET));
            assertEquals(resultTopic + " publish", producer.getName());
        });
    }

    @Test
    public void testFromKafkaToAppWithParentSpan() {
        String parentTopic = topic + "-parent";
        MyAppReceivingData bean = runApplication(getKafkaSinkConfigForMyAppReceivingData(parentTopic),
                MyAppReceivingData.class);

        RecordHeaders headers = new RecordHeaders();
        try (Scope ignored = Context.current().makeCurrent()) {
            Tracer tracer = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging");
            Span span = tracer.spanBuilder("producer").setSpanKind(SpanKind.PRODUCER).startSpan();
            Context current = Context.current().with(span);
            GlobalOpenTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(current, headers, (carrier, key, value) -> carrier.add(key, value.getBytes()));
            span.end();
        }
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(parentTopic, null, null, "a-key", i, headers), 10);

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
            assertEquals(8, consumer.getAttributes().size());
            assertEquals("kafka", consumer.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));
            assertEquals(parentTopic, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("kafka-consumer-stuff", consumer.getAttributes().get(MESSAGING_CLIENT_ID));
            assertEquals(0, consumer.getAttributes().get(MESSAGING_KAFKA_MESSAGE_OFFSET));
            assertEquals(parentTopic + " receive", consumer.getName());
        });
    }

    @Test
    public void testFromKafkaToAppWithNoParent() {
        MyAppReceivingData bean = runApplication(
                getKafkaSinkConfigForMyAppReceivingData(topic), MyAppReceivingData.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, null, null, "a-key", i), 10);

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

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppGeneratingData() {
        return kafkaConfig("mp.messaging.outgoing.kafka", true)
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", topic);
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppGeneratingDataWithStructuredCloudEvent(String mode) {
        return kafkaConfig("mp.messaging.outgoing.kafka", true)
                .put("value.serializer", StringSerializer.class.getName())
                .put("topic", topic)
                .put("cloud-events-mode", mode);
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppProcessingData(String resultTopic, String parentTopic) {
        return kafkaConfig("mp.messaging.outgoing.kafka", true)
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", resultTopic)
                .withPrefix("mp.messaging.incoming.source")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("topic", parentTopic)
                .put("auto.offset.reset", "earliest");
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppReceivingData(String topic) {
        return kafkaConfig("mp.messaging.incoming.stuff", true)
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("topic", topic)
                .put("auto.offset.reset", "earliest");
    }

    @ApplicationScoped
    public static class MyAppGeneratingData {
        @Outgoing("kafka")
        public Flow.Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }
    }

    @ApplicationScoped
    public static class MyAppGeneratingCloudEventData {
        @Outgoing("kafka")
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
        @Outgoing("kafka")
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
