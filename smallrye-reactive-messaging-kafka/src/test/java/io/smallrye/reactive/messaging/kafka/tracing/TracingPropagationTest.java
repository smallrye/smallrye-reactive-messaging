package io.smallrye.reactive.messaging.kafka.tracing;

import static io.smallrye.reactive.messaging.kafka.companion.RecordQualifiers.until;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
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
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class TracingPropagationTest extends KafkaCompanionTestBase {

    private InMemorySpanExporter testExporter;
    private SpanProcessor spanProcessor;

    @BeforeEach
    public void setup() {
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
    public void testFromAppToKafka() {
        List<Context> contexts = new CopyOnWriteArrayList<>();

        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic,
                m -> m.plug(until(10L, Duration.ofMinutes(1), null))
                        .onItem().invoke(record -> {
                            contexts.add(GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                                    .extract(Context.current(), record.headers(), new HeaderExtractAdapter()));
                        }));
        runApplication(getKafkaSinkConfigForMyAppGeneratingData(), MyAppGeneratingData.class);

        await().until(() -> consumed.getRecords().size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(consumed.getRecords()).allSatisfy(record -> {
            assertThat(record.value()).isNotNull();
            values.add(record.value());
        });
        assertThat(values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

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

    @Test
    public void testFromKafkaToAppToKafka() {
        List<Context> receivedContexts = new CopyOnWriteArrayList<>();
        String resultTopic = topic + "-result";
        String parentTopic = topic + "-parent";
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(resultTopic,
                m -> m.plug(until(10L, Duration.ofMinutes(1), null))
                        .onItem().invoke(record -> {
                            receivedContexts.add(GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                                    .extract(Context.current(), record.headers(), new HeaderExtractAdapter()));
                        }));
        MyAppProcessingData bean = runApplication(getKafkaSinkConfigForMyAppProcessingData(resultTopic, parentTopic),
                MyAppProcessingData.class);

        List<SpanContext> producedSpanContexts = new CopyOnWriteArrayList<>();
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(parentTopic, null, null, "a-key", i,
                        createTracingSpan(producedSpanContexts, parentTopic)), 10);

        await().until(() -> consumed.getRecords().size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(consumed.getRecords()).allSatisfy(record -> {
            assertThat(record.value()).isNotNull();
            values.add(record.value());
        });
        assertThat(values).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

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
                assertThat(data.getAttributes().get(SemanticAttributes.MESSAGING_KAFKA_CONSUMER_GROUP)).isNotNull();
                assertThat(data.getAttributes().get(AttributeKey.stringKey("messaging.consumer_id"))).isNotNull();
                // Need to skip the spans created during @Incoming processing
                continue;
            }
            assertThat(data.getSpanId()).isIn(receivedSpanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getTraceId()).isIn(producedTraceIds);
            assertThat(data.getKind()).isEqualByComparingTo(SpanKind.PRODUCER);
            outgoingParentIds.add(data.getParentSpanId());
        }

        // Assert span created on Kafka record is the parent of consumer span we create
        assertThat(producedSpanContexts.stream()
                .map(SpanContext::getSpanId)).containsExactlyElementsOf(incomingParentIds);

        // Assert consumer span is the parent of the producer span we received in Kafka
        assertThat(spanIds.stream())
                .containsExactlyElementsOf(outgoingParentIds);
    }

    @Test
    public void testFromKafkaToAppWithParentSpan() {
        String parentTopic = topic + "-parent";
        String stuffTopic = topic + "-stuff";
        MyAppReceivingData bean = runApplication(getKafkaSinkConfigForMyAppReceivingData(parentTopic),
                MyAppReceivingData.class);

        List<SpanContext> producedSpanContexts = new CopyOnWriteArrayList<>();

        companion.produceIntegers().usingGenerator(
                i -> new ProducerRecord<>(parentTopic, null, null, "a-key", i,
                        createTracingSpan(producedSpanContexts, stuffTopic)),
                10);

        await().until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<String> producedTraceIds = producedSpanContexts.stream()
                .map(SpanContext::getTraceId)
                .collect(Collectors.toList());
        assertThat(producedTraceIds).hasSize(10);

        assertThat(bean.tracing()).hasSizeGreaterThanOrEqualTo(10);
        assertThat(bean.tracing()).doesNotContainNull().doesNotHaveDuplicates();

        List<String> receivedTraceIds = bean.tracing().stream()
                .map(tracingMetadata -> Span.fromContext(tracingMetadata.getCurrentContext()).getSpanContext().getTraceId())
                .collect(Collectors.toList());
        assertThat(receivedTraceIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);
        assertThat(receivedTraceIds).containsExactlyInAnyOrderElementsOf(producedTraceIds);

        List<String> spanIds = new ArrayList<>();

        for (TracingMetadata tracing : bean.tracing()) {
            spanIds.add(Span.fromContext(tracing.getCurrentContext()).getSpanContext().getSpanId());

            assertThat(tracing.getPreviousContext()).isNotNull();
            Span previousSpan = Span.fromContextOrNull(tracing.getPreviousContext());
            assertThat(previousSpan).isNotNull();
            assertThat(previousSpan.getSpanContext().getTraceId())
                    .isEqualTo(Span.fromContext(tracing.getCurrentContext()).getSpanContext().getTraceId());
            assertThat(previousSpan.getSpanContext().getSpanId())
                    .isNotEqualTo(Span.fromContext(tracing.getCurrentContext()).getSpanContext().getSpanId());
        }

        assertThat(spanIds).doesNotContainNull().doesNotHaveDuplicates().hasSizeGreaterThanOrEqualTo(10);

        List<String> parentIds = bean.tracing().stream()
                .map(tracingMetadata -> Span.fromContextOrNull(tracingMetadata.getPreviousContext())
                        .getSpanContext().getSpanId())
                .collect(Collectors.toList());

        assertThat(producedSpanContexts.stream()
                .map(SpanContext::getSpanId)).containsExactlyElementsOf(parentIds);

        for (SpanData data : testExporter.getFinishedSpanItems()) {
            assertThat(data.getSpanId()).isIn(spanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getKind()).isEqualByComparingTo(SpanKind.CONSUMER);
            assertThat(data.getParentSpanId()).isNotNull();
            assertThat(data.getParentSpanId()).isIn(parentIds);
        }
    }

    @Test
    public void testFromKafkaToAppWithNoParent() {
        MyAppReceivingData bean = runApplication(
                getKafkaSinkConfigForMyAppReceivingData(topic), MyAppReceivingData.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, null, null, "a-key", i), 10);

        await().until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(bean.tracing()).hasSizeGreaterThanOrEqualTo(10);
        assertThat(bean.tracing()).doesNotContainNull().doesNotHaveDuplicates();
        List<String> spanIds = new ArrayList<>();

        for (TracingMetadata tracing : bean.tracing()) {
            spanIds.add(Span.fromContext(tracing.getCurrentContext()).getSpanContext().getSpanId());
            assertThat(Span.fromContextOrNull(tracing.getPreviousContext())).isNull();
        }

        assertThat(spanIds).doesNotContainNull().doesNotHaveDuplicates().hasSizeGreaterThanOrEqualTo(10);

        for (SpanData data : testExporter.getFinishedSpanItems()) {
            assertThat(data.getSpanId()).isIn(spanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getKind()).isEqualByComparingTo(SpanKind.CONSUMER);
        }
    }

    private Iterable<Header> createTracingSpan(List<SpanContext> spanContexts, String topic) {
        RecordHeaders proposedHeaders = new RecordHeaders();
        final Span span = KafkaConnector.TRACER.spanBuilder(topic).setSpanKind(SpanKind.PRODUCER).startSpan();
        final Context context = span.storeInContext(Context.current());
        GlobalOpenTelemetry.getPropagators()
                .getTextMapPropagator()
                .inject(context, proposedHeaders, (headers, key, value) -> {
                    if (headers != null) {
                        headers.remove(key).add(key, value.getBytes(StandardCharsets.UTF_8));
                    }
                });
        spanContexts.add(span.getSpanContext());
        return proposedHeaders;
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppGeneratingData() {
        return kafkaConfig("mp.messaging.outgoing.kafka", true)
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", topic);
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
        public Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingData {
        private final List<TracingMetadata> tracingMetadata = new ArrayList<>();

        @Incoming("source")
        @Outgoing("kafka")
        public Message<Integer> processMessage(Message<Integer> input) {
            tracingMetadata.add(input.getMetadata(TracingMetadata.class).orElse(TracingMetadata.empty()));
            return input.withPayload(input.getPayload() + 1);
        }

        public List<TracingMetadata> tracing() {
            return tracingMetadata;
        }
    }

    @ApplicationScoped
    public static class MyAppReceivingData {
        private final List<TracingMetadata> tracingMetadata = new ArrayList<>();
        private final List<Integer> results = new CopyOnWriteArrayList<>();

        @Incoming("stuff")
        public CompletionStage<Void> consume(Message<Integer> input) {
            results.add(input.getPayload());
            tracingMetadata.add(input.getMetadata(TracingMetadata.class).orElse(TracingMetadata.empty()));
            return input.ack();
        }

        public List<Integer> list() {
            return results;
        }

        public List<TracingMetadata> tracing() {
            return tracingMetadata;
        }
    }

}
