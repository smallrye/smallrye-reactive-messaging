package io.smallrye.reactive.messaging.kafka.tracing;

import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_KAFKA_OFFSET;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_SYSTEM;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.*;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.DELAYED_RETRY_CAUSE;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.DELAYED_RETRY_CAUSE_CLASS_NAME;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.DELAYED_RETRY_COUNT;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.DELAYED_RETRY_OFFSET;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.DELAYED_RETRY_ORIGINAL_TIMESTAMP;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.DELAYED_RETRY_PARTITION;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.DELAYED_RETRY_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandlerTest;

public class KafkaFailureHandlerTracingTest extends KafkaCompanionTestBase {

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

    @Test
    public void testDelayedRetryStrategyWithTracing() {
        addBeans(KafkaDelayedRetryTopic.Factory.class, KafkaFailureHandlerTest.MyObservationCollector.class);
        List<String> delayedRetryTopics = List.of(getRetryTopic(topic, 2000), getRetryTopic(topic, 4000));
        KafkaFailureHandlerTest.MyReceiverBean bean = runApplication(getDelayedRetryConfig(topic, delayedRetryTopics)
                .with("tracing-enabled", true), KafkaFailureHandlerTest.MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(bean.list())
                        .hasSizeGreaterThanOrEqualTo(16)
                        .containsOnlyOnce(0, 1, 2, 4, 5, 7, 8)
                        .contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(25);

        List<SpanData> producerSpans = verifyProducerSpans(spans, delayedRetryTopics);

        List<SpanData> consumerSpans = verifyConsumerSpans(spans, delayedRetryTopics);

        verifyProducerConsumerParents(producerSpans, consumerSpans);

        verifyMultipleRetryTraces(spans);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .fromTopics(delayedRetryTopics.toArray(String[]::new));

        await().untilAsserted(() -> assertThat(records.getRecords()).hasSizeGreaterThanOrEqualTo(6));
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isIn(delayedRetryTopics);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_OFFSET).value())).isNotNull().isIn("3", "6", "9");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_ORIGINAL_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_COUNT)).isNotNull();
            assertThat(r.headers().lastHeader("traceparent")).isNotNull();
        });

        assertThat(isAlive()).isTrue();

    }

    private static void verifyProducerConsumerParents(List<SpanData> producerSpans, List<SpanData> consumerSpans) {
        // Verify that producer spans (delayed retry) have correct parent consumer spans
        for (SpanData producerSpan : producerSpans) {
            // Each producer span should have a parent consumer span
            String parentSpanId = producerSpan.getParentSpanId();
            assertThat(parentSpanId).isNotEqualTo(SpanId.getInvalid());

            // Find the parent consumer span
            List<SpanData> parentSpans = consumerSpans.stream()
                    .filter(cs -> cs.getSpanId().equals(parentSpanId))
                    .toList();
            assertThat(parentSpans).hasSize(1);

            // Verify they share the same trace ID
            SpanData parentConsumerSpan = parentSpans.get(0);
            assertThat(producerSpan.getTraceId()).isEqualTo(parentConsumerSpan.getTraceId());
        }
    }

    private List<SpanData> verifyProducerSpans(List<SpanData> spans, List<String> delayedRetryTopics) {
        List<SpanData> producerSpans = spans.stream()
                .filter(s -> s.getKind() == SpanKind.PRODUCER)
                .toList();
        assertThat(producerSpans).hasSize(9);

        // Filter only retry topic producer spans (not DLQ spans)
        List<SpanData> retryProducerSpans = producerSpans.stream()
                .filter(s -> delayedRetryTopics.stream()
                        .anyMatch(retryTopic -> s.getAttributes().get(MESSAGING_DESTINATION_NAME).equals(retryTopic)))
                .toList();

        assertThat(retryProducerSpans).hasSize(6);

        // Verify producer spans have correct attributes
        for (SpanData producerSpan : retryProducerSpans) {
            Attributes attrs = producerSpan.getAttributes();
            assertThat(attrs.get(MESSAGING_SYSTEM)).isEqualTo("kafka");
            assertThat(attrs.get(MESSAGING_OPERATION)).isEqualTo("publish");
            assertThat(attrs.get(MESSAGING_DESTINATION_NAME)).isIn(delayedRetryTopics);
            assertThat(attrs.get(MESSAGING_KAFKA_OFFSET)).isNotNull();
            // Verify span name follows pattern "{topic} publish"
            assertThat(producerSpan.getName()).endsWith(" publish");
        }
        return producerSpans;
    }

    private List<SpanData> verifyConsumerSpans(List<SpanData> spans, List<String> delayedRetryTopics) {
        // Verify span parent propagation
        // Filter to get consumer spans (parent spans from incoming messages)
        List<SpanData> consumerSpans = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .toList();
        assertThat(consumerSpans).hasSize(16); // 10 original + 6 retries

        // Verify consumer spans have correct attributes
        for (SpanData consumerSpan : consumerSpans) {
            Attributes attrs = consumerSpan.getAttributes();
            assertThat(attrs.get(MESSAGING_SYSTEM)).isEqualTo("kafka");
            assertThat(attrs.get(MESSAGING_OPERATION)).isEqualTo("receive");
            String destinationName = attrs.get(MESSAGING_DESTINATION_NAME);
            assertThat(destinationName).isNotNull();
            assertThat(destinationName).satisfiesAnyOf(
                    name -> assertThat(name).isEqualTo(topic),
                    name -> assertThat(name).isIn(delayedRetryTopics));
            assertThat(attrs.get(MESSAGING_KAFKA_OFFSET)).isNotNull();
            // Verify span name follows pattern "{topic} receive"
            assertThat(consumerSpan.getName()).endsWith(" receive");
        }
        return consumerSpans;
    }

    private void verifyMultipleRetryTraces(List<SpanData> spans) {
        // Group spans by trace ID to track individual message traces
        var spansByTrace = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER || s.getKind() == SpanKind.PRODUCER)
                .collect(Collectors.groupingBy(SpanData::getTraceId));

        // For messages that go through multiple retry levels (3, 6, 9)
        // Each trace should contain: original consumer -> retry producer -> retry consumer -> retry producer -> retry consumer
        List<List<SpanData>> multiRetryTraces = spansByTrace.values().stream()
                .filter(traceSpans -> traceSpans.size() >= 5) // At least 2 retries
                .toList();

        assertThat(multiRetryTraces).isNotEmpty();

        for (List<SpanData> traceSpans : multiRetryTraces) {
            // Sort spans by start time to follow the flow
            List<SpanData> sortedSpans = traceSpans.stream()
                    .sorted((s1, s2) -> Long.compare(s1.getStartEpochNanos(), s2.getStartEpochNanos()))
                    .toList();

            // Verify parent-child relationships
            for (int i = 1; i < sortedSpans.size(); i++) {
                SpanData currentSpan = sortedSpans.get(i);
                String parentSpanId = currentSpan.getParentSpanId();

                // Each span (except root) should have a parent in the same trace
                if (!parentSpanId.equals(SpanId.getInvalid())) {
                    boolean hasParent = sortedSpans.stream()
                            .anyMatch(s -> s.getSpanId().equals(parentSpanId));
                    assertThat(hasParent)
                            .as("Span should have parent in same trace")
                            .isTrue();
                }
            }
        }
    }

    @Test
    public void testDeadLetterQueueWithTracing() {
        addBeans(KafkaDeadLetterQueue.Factory.class, KafkaFailureHandlerTest.MyObservationCollector.class);
        String dlqTopic = topic + "-dlq";
        KafkaFailureHandlerTest.MyReceiverBean bean = runApplication(getDLQConfig(topic, dlqTopic)
                .with("tracing-enabled", true), KafkaFailureHandlerTest.MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        ConsumerTask<String, Integer> dlqRecords = companion.consumeIntegers().fromTopics(dlqTopic, 3)
                .awaitCompletion();

        // Verify traceparent header is present in DLQ messages
        assertThat(dlqRecords.getRecords()).allSatisfy(r -> {
            Header traceparent = r.headers().lastHeader("traceparent");
            assertThat(traceparent).isNotNull();
        });

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        List<SpanData> producerSpans = spans.stream()
                .filter(s -> s.getKind() == SpanKind.PRODUCER)
                .toList();

        assertThat(producerSpans).isNotEmpty();

        // Verify DLQ producer spans have correct parent consumer spans
        List<SpanData> consumerSpans = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .toList();

        for (SpanData producerSpan : producerSpans) {
            String parentSpanId = producerSpan.getParentSpanId();
            assertThat(parentSpanId).isNotEqualTo(SpanId.getInvalid());

            List<SpanData> parentSpans = consumerSpans.stream()
                    .filter(cs -> cs.getSpanId().equals(parentSpanId))
                    .toList();
            assertThat(parentSpans).hasSize(1);

            // Verify they share the same trace ID
            assertThat(producerSpan.getTraceId()).isEqualTo(parentSpans.get(0).getTraceId());
        }
    }

    private KafkaMapBasedConfig getDelayedRetryConfig(String topic, List<String> topics) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", UUID.randomUUID().toString());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "delayed-retry-topic");
        config.put("dead-letter-queue.topic", topic + "-dlq");
        config.put("delayed-retry-topic.topics", String.join(",", topics));

        return config;
    }

    private KafkaMapBasedConfig getDLQConfig(String topic, String dlqTopic) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", UUID.randomUUID().toString());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "dead-letter-queue");
        config.put("dead-letter-queue.topic", dlqTopic);

        return config;
    }

}
