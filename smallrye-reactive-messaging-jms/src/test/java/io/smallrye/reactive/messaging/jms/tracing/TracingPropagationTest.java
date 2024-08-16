package io.smallrye.reactive.messaging.jms.tracing;

import static io.opentelemetry.semconv.SemanticAttributes.*;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanId;
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
import io.smallrye.reactive.messaging.jms.JmsConnector;
import io.smallrye.reactive.messaging.jms.PayloadConsumerBean;
import io.smallrye.reactive.messaging.jms.ProducerBean;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class TracingPropagationTest extends JmsTestBase {
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
    public void testFromAppToJms() {
        String queue = "queue-one";
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class, ProducerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();
        await().until(() -> bean.list().size() >= 10);
        Assertions.assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(20, spans.size());

            assertEquals(20, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

            SpanData span = spans.get(0);
            assertEquals(SpanKind.PRODUCER, span.getKind());
            assertEquals(3, span.getAttributes().size());
            assertEquals("jms", span.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("publish", span.getAttributes().get(MESSAGING_OPERATION));
            assertEquals("ActiveMQQueue[" + queue + "]", span.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("ActiveMQQueue[" + queue + "] publish", span.getName());
        });
    }

    @Test
    @Disabled("Not working yet. ")
    public void testFromKafkaToAppToKafka() {
        String queue = "queue-one";
        String resultTopic = queue + "-result";
        String parentTopic = queue + "-parent";
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class, ProducerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();
        await().until(() -> bean.list().size() >= 10);
        Assertions.assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(20, spans.size());

            List<SpanData> parentSpans = spans.stream()
                    .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid()))
                    .collect(toList());
            assertEquals(20, parentSpans.size());

            for (SpanData parentSpan : parentSpans) {
                assertEquals(1,
                        spans.stream().filter(spanData -> spanData.getParentSpanId().equals(parentSpan.getSpanId())).count());
            }

            SpanData consumer = parentSpans.get(0);
            assertEquals(SpanKind.CONSUMER, consumer.getKind());
            assertEquals(8, consumer.getAttributes().size());
            assertEquals("jms", consumer.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));
            assertEquals(parentTopic, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("jms-consumer-source", consumer.getAttributes().get(MESSAGING_CLIENT_ID));
            assertEquals(0, consumer.getAttributes().get(MESSAGING_KAFKA_MESSAGE_OFFSET));
            assertEquals(parentTopic + " receive", consumer.getName());

            SpanData producer = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(consumer.getSpanId()))
                    .findFirst().get();
            assertEquals(SpanKind.PRODUCER, producer.getKind());
            assertEquals(5, producer.getAttributes().size());
            assertEquals("jms", producer.getAttributes().get(MESSAGING_SYSTEM));
            assertEquals("publish", producer.getAttributes().get(MESSAGING_OPERATION));
            assertEquals(resultTopic, producer.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals("publish", producer.getAttributes().get(MESSAGING_OPERATION));
            assertEquals("jms-producer-jms", producer.getAttributes().get(MESSAGING_CLIENT_ID));
            assertEquals(resultTopic + " publish", producer.getName());
        });
    }

    //    @Test
    //    public void testFromKafkaToAppWithParentSpan() {
    //        String parentTopic = topic + "-parent";
    //        MyAppReceivingData bean = runApplication(getKafkaSinkConfigForMyAppReceivingData(parentTopic),
    //                MyAppReceivingData.class);
    //
    //        RecordHeaders headers = new RecordHeaders();
    //        try (Scope ignored = Context.current().makeCurrent()) {
    //            Tracer tracer = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging");
    //            Span span = tracer.spanBuilder("producer").setSpanKind(SpanKind.PRODUCER).startSpan();
    //            Context current = Context.current().with(span);
    //            GlobalOpenTelemetry.getPropagators()
    //                    .getTextMapPropagator()
    //                    .inject(current, headers, (carrier, key, value) -> carrier.add(key, value.getBytes()));
    //            span.end();
    //        }
    //        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(parentTopic, null, null, "a-key", i, headers), 10);
    //
    //        await().until(() -> bean.results().size() >= 10);
    //        assertThat(bean.results()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    //
    //        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
    //        completableResultCode.whenComplete(() -> {
    //            // 1 Parent, 10 Children
    //            List<SpanData> spans = spanExporter.getFinishedSpanItems();
    //            assertEquals(11, spans.size());
    //
    //            // All should use the same Trace
    //            assertEquals(1, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());
    //
    //            List<SpanData> parentSpans = spans.stream()
    //                    .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid())).collect(toList());
    //            assertEquals(1, parentSpans.size());
    //
    //            for (SpanData parentSpan : parentSpans) {
    //                assertEquals(10,
    //                        spans.stream().filter(spanData -> spanData.getParentSpanId().equals(parentSpan.getSpanId())).count());
    //            }
    //
    //            SpanData producer = parentSpans.get(0);
    //            assertEquals(SpanKind.PRODUCER, producer.getKind());
    //
    //            SpanData consumer = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(producer.getSpanId()))
    //                    .findFirst().get();
    //            assertEquals(8, consumer.getAttributes().size());
    //            assertEquals("kafka", consumer.getAttributes().get(MESSAGING_SYSTEM));
    //            assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));
    //            assertEquals(parentTopic, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
    //            assertEquals("kafka-consumer-stuff", consumer.getAttributes().get(MESSAGING_CLIENT_ID));
    //            assertEquals(0, consumer.getAttributes().get(MESSAGING_KAFKA_MESSAGE_OFFSET));
    //            assertEquals(parentTopic + " receive", consumer.getName());
    //        });
    //    }
    //
    //    @Test
    //    public void testFromKafkaToAppWithNoParent() {
    //        MyAppReceivingData bean = runApplication(
    //                getKafkaSinkConfigForMyAppReceivingData(topic), MyAppReceivingData.class);
    //
    //        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, null, null, "a-key", i), 10);
    //
    //        await().until(() -> bean.results().size() >= 10);
    //        assertThat(bean.results()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    //
    //        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
    //        completableResultCode.whenComplete(() -> {
    //            List<SpanData> spans = spanExporter.getFinishedSpanItems();
    //            assertEquals(10, spans.size());
    //
    //            for (SpanData span : spans) {
    //                assertEquals(SpanKind.CONSUMER, span.getKind());
    //                assertEquals(SpanId.getInvalid(), span.getParentSpanId());
    //            }
    //        });
    //    }
    //
    //    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppGeneratingData() {
    //        return kafkaConfig("mp.messaging.outgoing.kafka", true)
    //                .put("value.serializer", IntegerSerializer.class.getName())
    //                .put("topic", topic);
    //    }
    //
    //    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppGeneratingDataWithStructuredCloudEvent(String mode) {
    //        return kafkaConfig("mp.messaging.outgoing.kafka", true)
    //                .put("value.serializer", StringSerializer.class.getName())
    //                .put("topic", topic)
    //                .put("cloud-events-mode", mode);
    //    }
    //
    //    private MapBasedConfig getKafkaSinkConfigForMyAppProcessingData(String resultTopic, String parentTopic) {
    //        return kafkaConfig("mp.messaging.outgoing.kafka", true)
    //                .put("value.serializer", IntegerSerializer.class.getName())
    //                .put("topic", resultTopic)
    //                .withPrefix("mp.messaging.incoming.source")
    //                .put("value.deserializer", IntegerDeserializer.class.getName())
    //                .put("key.deserializer", StringDeserializer.class.getName())
    //                .put("topic", parentTopic)
    //                .put("auto.offset.reset", "earliest");
    //    }
    //
    //    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppReceivingData(String topic) {
    //        return kafkaConfig("mp.messaging.incoming.stuff", true)
    //                .put("value.deserializer", IntegerDeserializer.class.getName())
    //                .put("key.deserializer", StringDeserializer.class.getName())
    //                .put("topic", topic)
    //                .put("auto.offset.reset", "earliest");
    //    }
    //
    //    @ApplicationScoped
    //    public static class MyAppGeneratingData {
    //        @Outgoing("kafka")
    //        public Flow.Publisher<Integer> source() {
    //            return Multi.createFrom().range(0, 10);
    //        }
    //    }
    //
    @ApplicationScoped
    public static class MyAppProcessingData {
        @Incoming("source")
        @Outgoing("jms")
        public Message<Integer> processMessage(Message<Integer> input) {
            return input.withPayload(input.getPayload() + 1);
        }
    }
    //
    //    @ApplicationScoped
    //    public static class MyAppReceivingData {
    //        private final List<Integer> results = new CopyOnWriteArrayList<>();
    //
    //        @Incoming("stuff")
    //        public CompletionStage<Void> consume(Message<Integer> input) {
    //            results.add(input.getPayload());
    //            return input.ack();
    //        }
    //
    //        public List<Integer> results() {
    //            return results;
    //        }
    //    }
}
