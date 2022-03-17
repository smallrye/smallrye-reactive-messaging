package io.smallrye.reactive.messaging.kafka.tracing;

import static io.smallrye.reactive.messaging.kafka.companion.RecordQualifiers.until;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
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
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;

public class BatchTracingPropagationTest extends KafkaCompanionTestBase {
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

        await().until(() -> bean.records().size() >= 10);
        assertThat(bean.records()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(11, spans.size());
        });
    }

    @Test
    public void testFromKafkaToAppWithNoParent() {
        MyAppReceivingData bean = runApplication(
                getKafkaSinkConfigForMyAppReceivingData(topic), MyAppReceivingData.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, null, null, "a-key", i), 10);

        await().until(() -> bean.records().size() >= 10);
        assertThat(bean.records()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());
        });
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppGeneratingData() {
        return kafkaConfig("mp.messaging.outgoing.kafka", true)
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", topic)
                .put("batch", true);
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppReceivingData(String topic) {
        return kafkaConfig("mp.messaging.incoming.stuff", true)
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("topic", topic)
                .put("auto.offset.reset", "earliest")
                .put("batch", true);
    }

    @ApplicationScoped
    public static class MyAppGeneratingData {

        @Outgoing("kafka")
        public Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }
    }

    @ApplicationScoped
    public static class MyAppReceivingData {
        private final List<Integer> results = new CopyOnWriteArrayList<>();

        @Incoming("stuff")
        public CompletionStage<Void> consume(KafkaRecordBatch<String, Integer> batchInput) {
            results.addAll(batchInput.getPayload());
            return batchInput.ack();
        }

        public List<Integer> records() {
            return results;
        }
    }
}
