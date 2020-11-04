package io.smallrye.reactive.messaging.kafka.tracing;

import static io.opentelemetry.trace.Span.Kind.CONSUMER;
import static io.opentelemetry.trace.Span.Kind.PRODUCER;
import static io.opentelemetry.trace.TracingContextUtils.withSpan;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.grpc.Context;
import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.context.propagation.DefaultContextPropagators;
import io.opentelemetry.exporters.inmemory.InMemorySpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.trace.*;
import io.opentelemetry.trace.propagation.HttpTraceContext;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;

public class TracingPropagationTest extends KafkaTestBase {

    private InMemorySpanExporter testExporter;
    private SpanProcessor spanProcessor;

    @BeforeAll
    static void setupOpenTelemetry() {
        // We need to set a propagator
        OpenTelemetry.setPropagators(
                DefaultContextPropagators.builder().addTextMapPropagator(HttpTraceContext.getInstance()).build());
    }

    @BeforeEach
    public void setup() {
        testExporter = InMemorySpanExporter.create();
        spanProcessor = SimpleSpanProcessor.newBuilder(testExporter).build();
        OpenTelemetrySdk.getTracerManagement().addSpanProcessor(spanProcessor);
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

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFromAppToKafka() {
        List<Map.Entry<String, Integer>> messages = new CopyOnWriteArrayList<>();
        List<Context> contexts = new CopyOnWriteArrayList<>();
        usage.consumeIntegersWithTracing("output", 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)),
                contexts::add);
        runApplication(getKafkaSinkConfigForMyAppGeneratingData(), MyAppGeneratingData.class);

        await().until(() -> messages.size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getValue()).isNotNull();
            values.add(entry.getValue());
        });
        assertThat(values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(contexts).hasSize(10);
        assertThat(contexts).doesNotContainNull().doesNotHaveDuplicates();

        List<String> spanIds = contexts.stream()
                .map(context -> TracingContextUtils.getSpanWithoutDefault(context).getContext().getSpanIdAsHexString())
                .collect(Collectors.toList());
        assertThat(spanIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);

        List<String> traceIds = contexts.stream()
                .map(context -> TracingContextUtils.getSpanWithoutDefault(context).getContext().getTraceIdAsHexString())
                .collect(Collectors.toList());
        assertThat(traceIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);

        for (SpanData data : testExporter.getFinishedSpanItems()) {
            assertThat(data.getSpanId()).isIn(spanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getTraceId()).isIn(traceIds);
            assertThat(data.getKind()).isEqualByComparingTo(PRODUCER);
        }
    }

    @Test
    public void testFromKafkaToAppToKafka() {
        List<Map.Entry<String, Integer>> messages = new CopyOnWriteArrayList<>();
        List<Context> receivedContexts = new CopyOnWriteArrayList<>();
        usage.consumeIntegersWithTracing("result-topic", 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)),
                receivedContexts::add);
        runApplication(getKafkaSinkConfigForMyAppProcessingData(), MyAppProcessingData.class);

        AtomicInteger count = new AtomicInteger();
        List<SpanContext> producedSpanContexts = new CopyOnWriteArrayList<>();
        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("some-topic", null, null, "a-key", count.getAndIncrement(),
                        createTracingSpan(producedSpanContexts, "some-topic")));

        await().atMost(Duration.ofMinutes(5)).until(() -> messages.size() >= 10);
        List<Integer> values = new ArrayList<>();
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getValue()).isNotNull();
            values.add(entry.getValue());
        });
        assertThat(values).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<String> producedTraceIds = producedSpanContexts.stream()
                .map(SpanContext::getTraceIdAsHexString)
                .collect(Collectors.toList());
        assertThat(producedTraceIds).hasSize(10);

        assertThat(receivedContexts).hasSize(10);
        assertThat(receivedContexts).doesNotContainNull().doesNotHaveDuplicates();

        List<String> receivedSpanIds = receivedContexts.stream()
                .map(context -> TracingContextUtils.getSpanWithoutDefault(context).getContext().getSpanIdAsHexString())
                .collect(Collectors.toList());
        assertThat(receivedSpanIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);

        List<String> receivedTraceIds = receivedContexts.stream()
                .map(context -> TracingContextUtils.getSpanWithoutDefault(context).getContext().getTraceIdAsHexString())
                .collect(Collectors.toList());
        assertThat(receivedTraceIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);
        assertThat(receivedTraceIds).containsExactlyInAnyOrderElementsOf(producedTraceIds);

        List<String> receivedParentSpanIds = new ArrayList<>();

        for (SpanData data : testExporter.getFinishedSpanItems()) {
            if (data.getKind().equals(CONSUMER)) {
                // Need to skip the spans created during @Incoming processing
                continue;
            }
            assertThat(data.getSpanId()).isIn(receivedSpanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getTraceId()).isIn(producedTraceIds);
            assertThat(data.getKind()).isEqualByComparingTo(PRODUCER);
            receivedParentSpanIds.add(data.getParentSpanId());
        }

        assertThat(producedSpanContexts.stream()
                .map(SpanContext::getSpanIdAsHexString)).containsExactlyElementsOf(receivedParentSpanIds);
    }

    @Test
    public void testFromKafkaToAppWithParentSpan() {
        MyAppReceivingData bean = runApplication(getKafkaSinkConfigForMyAppReceivingData("parent-stuff"),
                MyAppReceivingData.class);

        AtomicInteger count = new AtomicInteger();
        List<SpanContext> producedSpanContexts = new CopyOnWriteArrayList<>();

        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("parent-stuff", null, null, "a-key", count.getAndIncrement(),
                        createTracingSpan(producedSpanContexts, "stuff-topic")));

        await().until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<String> producedTraceIds = producedSpanContexts.stream()
                .map(SpanContext::getTraceIdAsHexString)
                .collect(Collectors.toList());
        assertThat(producedTraceIds).hasSize(10);

        assertThat(bean.tracing()).hasSizeGreaterThanOrEqualTo(10);
        assertThat(bean.tracing()).doesNotContainNull().doesNotHaveDuplicates();

        List<String> receivedTraceIds = bean.tracing().stream()
                .map(tracingMetadata -> tracingMetadata.getCurrentSpanContext().getTraceIdAsHexString())
                .collect(Collectors.toList());
        assertThat(receivedTraceIds).doesNotContainNull().doesNotHaveDuplicates().hasSize(10);
        assertThat(receivedTraceIds).containsExactlyInAnyOrderElementsOf(producedTraceIds);

        List<String> spanIds = new ArrayList<>();

        for (TracingMetadata tracing : bean.tracing()) {
            spanIds.add(tracing.getCurrentSpanContext().getSpanIdAsHexString());

            assertThat(tracing.getPreviousContext()).isNotNull();
            Span previousSpan = TracingContextUtils.getSpanWithoutDefault(tracing.getPreviousContext());
            assertThat(previousSpan).isNotNull();
            assertThat(previousSpan.getContext().getTraceIdAsHexString())
                    .isEqualTo(tracing.getCurrentSpanContext().getTraceIdAsHexString());
            assertThat(previousSpan.getContext().getSpanIdAsHexString())
                    .isNotEqualTo(tracing.getCurrentSpanContext().getSpanIdAsHexString());
        }

        assertThat(spanIds).doesNotContainNull().doesNotHaveDuplicates().hasSizeGreaterThanOrEqualTo(10);

        List<String> parentIds = bean.tracing().stream()
                .map(tracingMetadata -> TracingContextUtils.getSpanWithoutDefault(tracingMetadata.getPreviousContext())
                        .getContext().getSpanIdAsHexString())
                .collect(Collectors.toList());

        assertThat(producedSpanContexts.stream()
                .map(SpanContext::getSpanIdAsHexString)).containsExactlyElementsOf(parentIds);

        for (SpanData data : testExporter.getFinishedSpanItems()) {
            assertThat(data.getSpanId()).isIn(spanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getKind()).isEqualByComparingTo(CONSUMER);
            assertThat(data.getParentSpanId()).isNotNull();
            assertThat(data.getParentSpanId()).isIn(parentIds);
        }
    }

    @Test
    public void testFromKafkaToAppWithNoParent() {
        MyAppReceivingData bean = runApplication(
                getKafkaSinkConfigForMyAppReceivingData("no-parent-stuff"), MyAppReceivingData.class);

        AtomicInteger count = new AtomicInteger();

        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("no-parent-stuff", null, null, "a-key", count.getAndIncrement()));

        await().until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(bean.tracing()).hasSizeGreaterThanOrEqualTo(10);
        assertThat(bean.tracing()).doesNotContainNull().doesNotHaveDuplicates();
        List<String> spanIds = new ArrayList<>();

        for (TracingMetadata tracing : bean.tracing()) {
            spanIds.add(tracing.getCurrentSpanContext().getSpanIdAsHexString());
            assertThat(TracingContextUtils.getSpanWithoutDefault(tracing.getPreviousContext())).isNull();
        }

        assertThat(spanIds).doesNotContainNull().doesNotHaveDuplicates().hasSizeGreaterThanOrEqualTo(10);

        for (SpanData data : testExporter.getFinishedSpanItems()) {
            assertThat(data.getSpanId()).isIn(spanIds);
            assertThat(data.getSpanId()).isNotEqualTo(data.getParentSpanId());
            assertThat(data.getKind()).isEqualByComparingTo(CONSUMER);
        }
    }

    private Iterable<Header> createTracingSpan(List<SpanContext> spanContexts, String topic) {
        RecordHeaders proposedHeaders = new RecordHeaders();
        final Span span = KafkaConnector.TRACER.spanBuilder(topic).setSpanKind(PRODUCER).startSpan();
        final Context context = withSpan(span, Context.current());
        OpenTelemetry.getPropagators()
                .getTextMapPropagator()
                .inject(context, proposedHeaders, (headers, key, value) -> {
                    if (headers != null) {
                        headers.remove(key).add(key, value.getBytes(StandardCharsets.UTF_8));
                    }
                });
        spanContexts.add(span.getContext());
        return proposedHeaders;
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppGeneratingData() {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.outgoing.kafka", true);
        builder.put("value.serializer", IntegerSerializer.class.getName());
        builder.put("topic", "output");
        return builder.build();
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppProcessingData() {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.outgoing.kafka", true);
        builder.put("value.serializer", IntegerSerializer.class.getName());
        builder.put("topic", "result-topic");

        Map<String, Object> config = builder.build();

        builder = MapBasedConfig.builder("mp.messaging.incoming.source", true);
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("key.deserializer", StringDeserializer.class.getName());
        builder.put("topic", "some-topic");
        builder.put("auto.offset.reset", "earliest");

        config.putAll(builder.build());
        return new MapBasedConfig(config);
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppReceivingData(String topic) {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.stuff", true);
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("key.deserializer", StringDeserializer.class.getName());
        builder.put("topic", topic);
        builder.put("auto.offset.reset", "earliest");

        return builder.build();
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

        @Incoming("source")
        @Outgoing("kafka")
        public Message<Integer> processMessage(Message<Integer> input) {
            return input.withPayload(input.getPayload() + 1);
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
